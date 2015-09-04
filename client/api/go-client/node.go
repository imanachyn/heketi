//
// Copyright (c) 2015 The heketi Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package client

import (
	"bytes"
	"encoding/json"
	"github.com/heketi/heketi/apps/glusterfs"
	"github.com/heketi/heketi/utils"
	"net/http"
	"time"
)

func (c *Client) NodeAdd(request *glusterfs.NodeAddRequest) (*glusterfs.NodeInfoResponse, error) {

	// Marshal request to JSON
	buffer, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	// Create a request
	req, err := http.NewRequest("POST", c.host+"/nodes", bytes.NewBuffer(buffer))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	// Set token
	err = c.setToken(req)
	if err != nil {
		return nil, err
	}

	// Send request
	httpClient := &http.Client{}
	r, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if r.StatusCode != http.StatusAccepted {
		return nil, utils.GetErrorFromResponse(r)
	}

	// Wait for response
	r, err = c.waitForResponseWithTimer(r, time.Millisecond*250)
	if err != nil {
		return nil, err
	}
	if r.StatusCode != http.StatusOK {
		return nil, utils.GetErrorFromResponse(r)
	}

	// Read JSON response
	var node glusterfs.NodeInfoResponse
	err = utils.GetJsonFromResponse(r, &node)
	r.Body.Close()
	if err != nil {
		return nil, err
	}

	return &node, nil
}

func (c *Client) NodeInfo(id string) (*glusterfs.NodeInfoResponse, error) {

	// Create request
	req, err := http.NewRequest("GET", c.host+"/nodes/"+id, nil)
	if err != nil {
		return nil, err
	}

	// Set token
	err = c.setToken(req)
	if err != nil {
		return nil, err
	}

	// Get info
	httpClient := &http.Client{}
	r, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if r.StatusCode != http.StatusOK {
		return nil, utils.GetErrorFromResponse(r)
	}

	// Read JSON response
	var node glusterfs.NodeInfoResponse
	err = utils.GetJsonFromResponse(r, &node)
	r.Body.Close()
	if err != nil {
		return nil, err
	}

	return &node, nil
}

func (c *Client) NodeDelete(id string) error {

	// Create a request
	req, err := http.NewRequest("DELETE", c.host+"/nodes/"+id, nil)
	if err != nil {
		return err
	}

	// Set token
	err = c.setToken(req)
	if err != nil {
		return err
	}

	// Send request
	httpClient := &http.Client{}
	r, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	if r.StatusCode != http.StatusAccepted {
		return utils.GetErrorFromResponse(r)
	}

	// Wait for response
	r, err = c.waitForResponseWithTimer(r, time.Millisecond*250)
	if err != nil {
		return err
	}
	if r.StatusCode != http.StatusNoContent {
		return utils.GetErrorFromResponse(r)
	}

	return nil
}
