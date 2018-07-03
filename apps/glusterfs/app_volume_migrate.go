package glusterfs

import (
	"fmt"
	"net/http"

	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/utils"
)

func (a *App) VolumeMigrate(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	volID := vars["id"]

	var msg api.VolumeMigrateRequest

	err := utils.GetJsonFromRequest(r, &msg)
	if err != nil {
		http.Error(w, "request unable to be parsed", http.StatusUnprocessableEntity)
		return
	}

	err = msg.Validate()
	if err != nil {
		http.Error(w, "validation failed: "+err.Error(), http.StatusBadRequest)
		logger.LogError("validation failed: " + err.Error())
		return
	}

	var volume *VolumeEntry
	err = a.db.View(func(tx *bolt.Tx) error {
		var err error
		volume, err = NewVolumeEntryFromId(tx, volID)
		if err == ErrNotFound || !volume.Visible() {
			http.Error(w, err.Error(), http.StatusNotFound)
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

	op := NewVolumeMigrateOperation(volume, a.db, msg.NodeId)
	if err := AsyncHttpOperation(a, w, r, op); err != nil {
		http.Error(w, fmt.Sprintf("Failed migrate volume %v: %v", volID, err), http.StatusInternalServerError)
		return
	}
}
