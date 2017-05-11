package cloudinsight

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
	"log"
	"io"
)

// API XXX
type API struct {
	ciURL      string
	licenseKey string
	version    string
	client     *http.Client
}

func NewAPI(ciURL string, licenseKey string, timeout time.Duration, proxy ...string) *API {
	ciURL = strings.TrimSuffix(ciURL, "/")
	client := http.Client{
		Timeout: timeout,
	}
	if len(proxy) > 0 && proxy[0] != "" {
		proxyURL, err := url.Parse(proxy[0])
		if err != nil {
			log.Fatalf("Error parsing proxy URL %s, %s", proxy[0], err.Error())
		}
		client.Transport = &http.Transport{
			Proxy:           http.ProxyURL(proxyURL),
			TLSClientConfig: &tls.Config{},
		}
	}
	
	api := &API{
		ciURL:      ciURL,
		licenseKey: licenseKey,
		client:     &client,
	}
	return api
}

// Post sends the metrics to Cloud-insight.
func (api *API) Post(path string, body io.Reader) error {
	req, err := http.NewRequest("POST", path, body)
	if err != nil {
		return fmt.Errorf("error @post-data: unable to create http.Request, %s", err.Error())
	}
	
	resp, err := api.do(req)
	defer func() {
		if resp != nil {
			err := resp.Body.Close()
			if err != nil {
				// Warn
				fmt.Errorf("failed to close the HTTP Response, %s", err.Error())
			}
		}
	}()
	
	if err != nil {
		return fmt.Errorf("error @post-data:  %s", err.Error())
	}
	
	if resp.StatusCode < 200 || resp.StatusCode > 209 {
		return fmt.Errorf("error @post-data: received bad status code, %d", resp.StatusCode)
	}
	
	return nil
}


func (api *API) do(req *http.Request) (resp *http.Response, err error) {
	req.Header.Add("User-Agent", fmt.Sprintf("Cloudinsight Agent/%s", api.version))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Encoding", "deflate")
	req.Header.Add("Accept", "text/html, */*")
	
	resp, err = api.client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetURL gets URL according to msgType(metrics, service_checks or series).
func (api *API) GetURL(msgType string) string {
	q := url.Values{
		"license_key": []string{api.licenseKey},
	}
	
	switch msgType {
	case "metrics":
		return fmt.Sprintf("%s/infrastructure/metrics?%s", api.ciURL, q.Encode())
	case "service_checks":
		return fmt.Sprintf("%s/infrastructure/service_checks?%s", api.ciURL, q.Encode())
	case "series":
		return fmt.Sprintf("%s/infrastructure/series?%s", api.ciURL, q.Encode())
	default:
		return ""
	}
}
