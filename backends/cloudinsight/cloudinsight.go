package cloudinsight

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func NewCiBackend(conf *CiConfig) *CiBackend {
	return &CiBackend{
		api:    NewAPI(conf.ciURL, conf.licenseKey, conf.timeout, conf.proxy),
		config: conf,
	}
}

type CiBackend struct {
	api    *API
	config *CiConfig
}

type Payload struct {
	Series []interface{} `json:"series"`
}

type CiConfig struct {
	forwarderAddr string
	ciURL         string
	licenseKey    string
	version       string
	timeout       time.Duration
	proxy         string
	client        *http.Client
}

func (b *CiBackend) Flush(metrics []interface{}) error {
	start := time.Now()
	payload := Payload{}
	payload.Series = metrics

	dataBytes, err := json.Marshal(&payload)
	if err != nil {
		return fmt.Errorf("unable to marshal data, %s", err.Error())
	}
	compressed := compress(dataBytes)
	elapsed := time.Since(start)

	err = b.api.Post(b.api.GetURL("metrics"), &compressed)
	if err == nil {
		fmt.Printf("Post batch of %d metrics in %s \n", len(metrics), elapsed)
	}
	return err
}

func compress(b []byte) bytes.Buffer {
	var buf bytes.Buffer
	comp := zlib.NewWriter(&buf)
	comp.Write(b)
	comp.Close()
	return buf
}
