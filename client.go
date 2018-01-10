package quorum

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type HTTPClient struct {
	client *http.Client
}

func NewHTTPClient() *HTTPClient {
	return &HTTPClient{
		client: &http.Client{},
	}
}

func (d *HTTPClient) Join(ctx context.Context, target string, jreq *JoinRequest) (*JoinResponse, error) {
	encodedArgs, err := json.Marshal(jreq)
	if err != nil {
		return nil, fmt.Errorf("Json error: %v", err)
	}

	bodyReader := bytes.NewReader(encodedArgs)

	req, err := http.NewRequest("POST", "http://"+target+"/v1/join", bodyReader)
	if err != nil {
		return nil, fmt.Errorf("Error creating request: %v", err)
	}

	req = req.WithContext(ctx)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Error from http requests: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Error parsing http response: %v", err)
	}

	var response *JoinResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, fmt.Errorf("Error umarshaling json response: %v", err)
	} else {
		return response, nil
	}
}

func (d *HTTPClient) Leave(ctx context.Context, target string, lreq *LeaveRequest) (*LeaveResponse, error) {
	encodedArgs, err := json.Marshal(lreq)
	if err != nil {
		return nil, fmt.Errorf("Json error: %v", err)
	}

	bodyReader := bytes.NewReader(encodedArgs)

	req, err := http.NewRequest("POST", "http://"+target+"/v1/leave", bodyReader)
	if err != nil {
		return nil, fmt.Errorf("Error creating request: %v", err)
	}

	req = req.WithContext(ctx)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Error from http requests: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Error parsing http response: %v", err)
	}

	var response *LeaveResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, fmt.Errorf("Error umarshaling json response: %v", err)
	} else {
		return response, nil
	}
}
