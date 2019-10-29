package main

import (
	"bytes"
	"errors"
	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	influxdb "github.com/influxdata/influxdb1-client/v2"
	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
	"github.com/spf13/viper"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"
)

type InfluxConfig struct {
	Url             string
	User            string
	Password        string
	Database        string
	RetentionPolicy string
	Precision       string
	Consistency     string
}

type NatsConfig struct {
	Url string
}

type StanConfig struct {
	ClusterID   string
	ClientID    string
	AckWait     string
	DurableName string
	Subject     string
}

type Config struct {
	Nats   NatsConfig
	Stan   StanConfig
	Influx InfluxConfig
}

func DefaultConfig() Config {
	return Config{
		Nats: NatsConfig{
			Url: nats.DefaultURL,
		},
		Stan: StanConfig{
			ClusterID:   "test-cluster",
			ClientID:    "influxdb-nats-writer",
			AckWait:     "60s",
			DurableName: "influxdb-writer-durable",
			Subject:     "telegraf",
		},
		Influx: InfluxConfig{
			Url:       "http://localhost:8086",
			Database:  "telegraf",
			Precision: "ns",
		},
	}
}

type Writer struct {
	nc         *nats.Conn
	sc         stan.Conn
	influx     influxdb.Client
	httpClient http.Client
	userAgent  string
	influxUrl  *url.URL
	C          *Config
}

func NewWriter(C *Config) (*Writer, error) {
	w := &Writer{}
	var err error
	log.Printf("Trying to connect to NATS at %s", C.Nats.Url)
	w.nc, err = nats.Connect(C.Nats.Url)

	if err != nil {
		return nil, err
	}

	log.Printf("Connecting to STAN cluster %s with client ID %s", C.Stan.ClusterID, C.Stan.ClientID)
	w.sc, err = stan.Connect(C.Stan.ClusterID, C.Stan.ClientID, stan.NatsConn(w.nc))

	if err != nil {
		w.nc.Close()
		return nil, err
	}

	log.Printf("Connecting to InfluxDB at %s", C.Influx.Url)
	w.influx, err = influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr: C.Influx.Url,
	})
	if err != nil {
		w.sc.Close()
		w.nc.Close()
		return nil, err
	}

	w.influxUrl, err = url.Parse(C.Influx.Url)
	if err != nil {
		return nil, err
	}
	w.influxUrl.Path = path.Join(w.influxUrl.Path, "write")
	w.userAgent = "influx-writer"
	w.httpClient = http.Client{}
	w.C = C
	return w, nil
}

func (w Writer) Subscribe(onMessage stan.MsgHandler) (stan.Subscription, error) {
	aw, _ := time.ParseDuration(w.C.Stan.AckWait)
	return w.sc.Subscribe(w.C.Stan.Subject, onMessage,
		stan.SetManualAckMode(), stan.AckWait(aw),
		stan.DurableName(w.C.Stan.DurableName))
}

func (w Writer) WritePoints(b []byte) error {
	req, err := http.NewRequest("POST", w.influxUrl.String(),
		bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", w.userAgent)
	if w.C.Influx.User != "" {
		req.SetBasicAuth(w.C.Influx.User, w.C.Influx.Password)
	}

	params := req.URL.Query()
	params.Set("db", w.C.Influx.Database)
	params.Set("rp", w.C.Influx.RetentionPolicy)
	params.Set("precision", w.C.Influx.Precision)
	params.Set("consistency", w.C.Influx.Consistency)
	req.URL.RawQuery = params.Encode()

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		var err = errors.New(string(body))
		return err
	}

	return nil
}

func (w Writer) Close() {
	log.Println("Closing connections...")
	w.influx.Close()
	w.sc.Close()
	w.nc.Close()
}

func main() {
	C := DefaultConfig()

	errorOccured := false

	errorChan := make(chan bool, 1)
	subChan := make(chan bool, 1)
	configChan := make(chan bool, 1)
	configChan <- true

	var rv = viper.New()
	rv.SetConfigName("writer")
	rv.AddConfigPath("$HOME/.config")
	rv.AddConfigPath(".")
	err := rv.ReadInConfig()
	if err != nil {
		log.Printf("Config file not found, using default settings")
	} else {
		err = rv.Unmarshal(&C)
		if err != nil {
			log.Fatal(err)
		}
	}

	var w *Writer

	onMessage := func(m *stan.Msg) {
		if errorOccured {
			return
		}
		err = w.WritePoints(m.Data)
		if err != nil {
			log.Printf("Error occured: %s", err)
			errorChan <- true
		} else {
			errorChan <- false
			m.Ack()
		}
	}

	var sub stan.Subscription
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	for {
		select {
		case <-configChan:
			if w != nil {
				w.Close()
			}
			w, err = NewWriter(&C)
			if err != nil {
				log.Fatal("Could not init Writer")
			}
			subChan <- true
		case <-subChan:
			log.Printf("Subscribing to subject %s", C.Stan.Subject)
			sub, err = w.Subscribe(onMessage)

			if err != nil {
				log.Fatal(err)
				subChan <- true
				continue
			}
			errorChan <- false
		case errorOccured = <-errorChan:
			if !errorOccured {
				continue
			}
			log.Print("Closing subscription due to error...")
			sub.Close()

			log.Print("Sleeping for 60 seconds...")
			time.Sleep(60 * time.Second)
			subChan <- true
		case sig := <-signalChan:
			switch sig {
			case syscall.SIGHUP:
				configChan <- true
			case syscall.SIGINT:
				fallthrough
			case syscall.SIGTERM:
				w.Close()
				return
			}
		}
	}
}
