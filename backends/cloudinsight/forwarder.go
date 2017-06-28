package cloudinsight

import (
	"net"
	"net/http"
	"time"

	"fmt"
	"log"
)

func NewForwarder(conf *CiConfig) *Forwarder {
	return &Forwarder{
		api:    NewAPI(conf.ciURL, conf.licenseKey, conf.timeout, conf.proxy),
		config: conf,
	}
}

// Forwarder sends the metrics to Cloudinsight data center, which is collected by Collector and Statsd.
type Forwarder struct {
	api    *API
	config *CiConfig
}

//type ForwarderConfig struct {
//	forwarderAddr string
//	ciURL      string
//	licenseKey string
//	version    string
//	timeout    time.Duration
//	proxy      string
//	client     *http.Client
//}

func (f *Forwarder) metricHandler(w http.ResponseWriter, r *http.Request) {
	err := f.api.Post(f.api.GetURL("metrics"), r.Body)
	if err != nil {
		fmt.Errorf("Error occurred when posting Payload. %s", err)
	}
}

// Run runs a http server listening to 10010 as default.
func (f *Forwarder) Run(shutdown chan struct{}) error {
	http.HandleFunc("/infrastructure/metrics", f.metricHandler)

	http.HandleFunc("/infrastructure/series", func(w http.ResponseWriter, r *http.Request) {
		// TODO
	})

	http.HandleFunc("/infrastructure/service_checks", func(w http.ResponseWriter, r *http.Request) {
		// TODO
	})

	s := &http.Server{
		Handler:        nil,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	addr := f.config.forwarderAddr

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	fmt.Println("Forwarder listening on:", addr)

	go func() {
		if err := s.Serve(l); err != nil {
			log.Fatal(err)
		}
	}()

	select {
	case <-shutdown:
		fmt.Println("Forwarder server thread exit")
		if err := l.Close(); err != nil {
			return err
		}
	}

	return nil
}
