package client

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/utils"
)

func (c *Client) VolumeMigrate(id string, request *api.VolumeMigrateRequest) (*api.VolumeInfoResponse, error) {
	// Marshal request to JSON
	buffer, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	// Create a request
	req, err := http.NewRequest("POST", c.host+"/volumes/"+id+"/migrate", bytes.NewBuffer(buffer))
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
	r, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusAccepted {
		return nil, utils.GetErrorFromResponse(r)
	}

	// Wait for response
	r, err = c.waitForResponseWithTimer(r, time.Second)
	if err != nil {
		return nil, err
	}
	if r.StatusCode != http.StatusOK {
		return nil, utils.GetErrorFromResponse(r)
	}

	// Read JSON response
	var volume api.VolumeInfoResponse
	err = utils.GetJsonFromResponse(r, &volume)
	if err != nil {
		return nil, err
	}

	return &volume, nil
}
