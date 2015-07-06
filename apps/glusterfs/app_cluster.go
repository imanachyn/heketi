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
	"errors"
	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
	"github.com/heketi/heketi/utils"
	"net/http"
)

var (
	ErrNotFound = errors.New("Id not found")
)

func (a *App) ClusterCreate(w http.ResponseWriter, r *http.Request) {

	// Create a new ClusterInfo
	entry := NewClusterEntry()
	entry.Info.Id = utils.GenUUID()

	// Convert entry to bytes
	buffer, err := entry.Marshal()
	if err != nil {
		http.Error(w, "Unable to create cluster", http.StatusInternalServerError)
		return
	}

	// Add cluster to db
	err = a.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET_CLUSTER))
		if b == nil {
			logger.Error("Unable to save new cluster information in db")
			return errors.New("Unable to open bucket")
		}

		err = b.Put([]byte(entry.Info.Id), buffer)
		if err != nil {
			logger.Error("Unable to save new cluster information in db")
			return err
		}

		return nil

	})

	if err != nil {
		logger.Err(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send back we created it (as long as we did not fail)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(entry.Info); err != nil {
		panic(err)
	}
}

func (a *App) ClusterList(w http.ResponseWriter, r *http.Request) {

	var list ClusterListResponse
	list.Clusters = make([]string, 0)

	// Get all the cluster ids from the DB
	err := a.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET_CLUSTER))
		if b == nil {
			logger.Error("Unable to access db")
			return errors.New("Unable to open bucket")
		}

		b.ForEach(func(k, v []byte) error {
			list.Clusters = append(list.Clusters, string(k))
			return nil
		})

		return nil
	})

	if err != nil {
		logger.Err(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send list back
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(list); err != nil {
		panic(err)
	}
}

func (a *App) ClusterInfo(w http.ResponseWriter, r *http.Request) {

	// Get the id from the URL
	vars := mux.Vars(r)
	id := vars["id"]

	// Get info from db
	var entry ClusterEntry
	err := a.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET_CLUSTER))
		if b == nil {
			logger.Error("Unable to access db")
			return errors.New("Unable to open bucket")
		}

		val := b.Get([]byte(id))
		if val == nil {
			return ErrNotFound
		}

		return entry.Unmarshal(val)
	})
	if err == ErrNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// Write msg
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(entry.Info); err != nil {
		panic(err)
	}

}
