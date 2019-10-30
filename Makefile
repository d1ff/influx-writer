PHONY:=debug release

debug:
	dep ensure -vendor-only
	go build ./influx_writer.go

release:
	dep ensure -vendor-only
	go build -ldflags="-s -w" ./influx_writer.go
	upx influx_writer
