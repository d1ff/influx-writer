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
	"path"
	"sync"
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

type config struct {
	Nats   NatsConfig
	Stan   StanConfig
	Influx InfluxConfig
}

func DefaultConfig() config {
	return config{
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

func main() {
	C := DefaultConfig()

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

	log.Printf("Trying to connect to NATS at %s", C.Nats.Url)
	nc, err := nats.Connect(C.Nats.Url)

	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Printf("Connecting to STAN cluster %s with client ID %s", C.Stan.ClusterID, C.Stan.ClientID)
	sc, err := stan.Connect(C.Stan.ClusterID, C.Stan.ClientID, stan.NatsConn(nc))

	if err != nil {
		log.Fatal(err)
	}
	defer sc.Close()

	log.Printf("Connecting to InfluxDB at %s", C.Influx.Url)
	influx, err := influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr: C.Influx.Url,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer influx.Close()

	errorOccured := false
	errorLocker := sync.Mutex{}
	errorCondition := sync.NewCond(&errorLocker)
	influxUrl, err := url.Parse(C.Influx.Url)
	if err != nil {
		log.Fatal(err)
	}
	influxUrl.Path = path.Join(influxUrl.Path, "write")
	userAgent := "influx-writer"
	httpClient := &http.Client{}

	writePoints := func(b []byte) error {
		req, err := http.NewRequest("POST", influxUrl.String(),
			bytes.NewBuffer(b))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "")
		req.Header.Set("User-Agent", userAgent)
		if C.Influx.User != "" {
			req.SetBasicAuth(C.Influx.User, C.Influx.Password)
		}

		params := req.URL.Query()
		params.Set("db", C.Influx.Database)
		params.Set("rp", C.Influx.RetentionPolicy)
		params.Set("precision", C.Influx.Precision)
		params.Set("consistency", C.Influx.Consistency)
		req.URL.RawQuery = params.Encode()

		resp, err := httpClient.Do(req)
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

	onMessage := func(m *stan.Msg) {
		if errorOccured {
			return
		}
		err = writePoints(m.Data)
		if err != nil {
			log.Printf("Error occured: %s", err)
			errorCondition.L.Lock()
			errorOccured = true
			errorCondition.Broadcast()
			errorCondition.L.Unlock()
		} else {
			m.Ack()
		}
	}

	aw, _ := time.ParseDuration(C.Stan.AckWait)

	for {
		log.Printf("Subscribing to subject %s", C.Stan.Subject)
		sub, err := sc.Subscribe(C.Stan.Subject, onMessage,
			stan.SetManualAckMode(), stan.AckWait(aw),
			stan.DurableName(C.Stan.DurableName))

		if err != nil {
			log.Fatal(err)
			continue
		}
		errorCondition.L.Lock()
		for !errorOccured {
			errorCondition.Wait()
		}
		errorCondition.L.Unlock()
		log.Print("Closing subscription due to error...")
		sub.Close()

		log.Print("Sleeping for 60 seconds...")
		time.Sleep(60 * time.Second)
		errorCondition.L.Lock()
		errorOccured = false
		errorCondition.L.Unlock()
	}
}
