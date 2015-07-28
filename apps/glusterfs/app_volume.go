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

package glusterfs

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
	"github.com/heketi/heketi/utils"
	"net/http"
	"time"
)

const (
	VOLUME_CREATE_MAX_SNAPSHOT_FACTOR = 100
)

func (a *App) VolumeCreate(w http.ResponseWriter, r *http.Request) {

	var msg VolumeCreateRequest
	err := utils.GetJsonFromRequest(r, &msg)
	if err != nil {
		http.Error(w, "request unable to be parsed", 422)
		return
	}

	// Check the message has devices
	if msg.Size < 1 {
		http.Error(w, "Invalid volume size", http.StatusBadRequest)
		return
	}
	if msg.Snapshot.Enable {
		if msg.Snapshot.Factor < 1 || msg.Snapshot.Factor > VOLUME_CREATE_MAX_SNAPSHOT_FACTOR {
			http.Error(w, "Invalid snapshot factor", http.StatusBadRequest)
			return
		}
	}

	// Check that the clusters requested are avilable
	err = a.db.View(func(tx *bolt.Tx) error {

		// Check we have clusters
		// :TODO: All we need to do is check for one instead of gathering all keys
		clusters, err := ClusterList(tx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}
		if len(clusters) == 0 {
			http.Error(w, fmt.Sprintf("No clusters configured"), http.StatusBadRequest)
			return ErrNotFound
		}

		// Check the clusters requested are correct
		for _, clusterid := range msg.Clusters {
			_, err := NewClusterEntryFromId(tx, clusterid)
			if err != nil {
				http.Error(w, fmt.Sprintf("Cluster id %v not found", clusterid), http.StatusBadRequest)
				return err
			}
		}

		return nil
	})
	if err != nil {
		return
	}

	// Create a volume entry
	vol := NewVolumeEntryFromRequest(&msg)

	// Add device in an asynchronous function
	a.asyncManager.AsyncHttpRedirectFunc(w, r, func() (string, error) {

		logger.Info("Creating volume %v", vol.Info.Id)
		err := vol.Create(a.db)
		if err != nil {
			logger.LogError("Failed to create volume %v", vol.Info.Id)
			return "", err
		}

		logger.Info("Created volume %v", vol.Info.Id)

		// Done
		return "/volumes/" + vol.Info.Id, nil
	})

}

func (a *App) VolumeInfo(w http.ResponseWriter, r *http.Request) {

	// Get device id from URL
	vars := mux.Vars(r)
	id := vars["id"]

	// Get device information
	var info *VolumeInfoResponse
	err := a.db.View(func(tx *bolt.Tx) error {
		entry, err := NewVolumeEntryFromId(tx, id)
		if err == ErrNotFound {
			http.Error(w, "Id not found", http.StatusNotFound)
			return err
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		info, err = entry.NewInfoResponse(tx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		return nil
	})
	if err != nil {
		return
	}

	// Write msg
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(info); err != nil {
		panic(err)
	}

}

func (a *App) VolumeDelete(w http.ResponseWriter, r *http.Request) {
	// Get the id from the URL
	vars := mux.Vars(r)
	id := vars["id"]

	// Get info from db
	err := a.db.Update(func(tx *bolt.Tx) error {

		// Access volume entry
		volume, err := NewVolumeEntryFromId(tx, id)
		if err == ErrNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return err
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		// Delete volume from cluster

		// Delete volume from db
		err = volume.Delete(tx)
		if err == ErrConflict {
			http.Error(w, err.Error(), http.StatusConflict)
			return err
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		return nil

	})
	if err != nil {
		return
	}

	a.asyncManager.AsyncHttpRedirectFunc(w, r, func() (string, error) {

		// Actually destroy the Volume here
		time.Sleep(time.Millisecond * 10)

		// If it fails for some reason, we will need to add to the DB again
		// or hold state on the entry "DELETING"

		// Show that the key has been deleted
		logger.Info("Deleted node [%s]", id)

		return "", nil

	})

}
