package glusterfs

import (
	"bytes"
	"github.com/gorilla/mux"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/utils"
	"github.com/heketi/tests"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
	"github.com/heketi/heketi/executors"
)

func TestVolumeMigrate(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	// Create a cluster
	err := setupSampleDbWithTopology(app,
		1,    // clusters
		10,   // nodes_per_cluster
		10,   // devices_per_node,
		5*TB, // disksize)
	)
	tests.Assert(t, err == nil)

	// Create a volume
	v := createSampleReplicaVolumeEntry(100, 2)
	tests.Assert(t, v != nil)
	err = v.Create(app.db, app.executor)
	tests.Assert(t, err == nil)

	// Keep a copy
	vc := &VolumeEntry{}
	*vc = *v

	var info api.VolumeInfoResponse
	r, err := http.Get(ts.URL + "/volumes/" + v.Info.Id)
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == http.StatusOK)
	err = utils.GetJsonFromResponse(r, &info)
	tests.Assert(t, err == nil)


	app.xo.MockVolumeInfo = func(host string, volume string) (*executors.Volume, error) {
		return mockVolumeInfoFromDb(app.db, volume)
	}
	app.xo.MockHealInfo = func(host string, volume string) (*executors.HealInfo, error) {
		return mockHealStatusFromDb(app.db, volume)
	}

	// JSON Request
	request := []byte(`{
       "node" : "` + info.Bricks[0].NodeId + `"
	}`)

	// Send request
	r, err = http.Post(ts.URL+"/volumes/"+v.Info.Id+"/migrate", "application/json", bytes.NewBuffer(request))
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == http.StatusAccepted)
	location, err := r.Location()
	tests.Assert(t, err == nil)

	// Query queue until finished
	for {
		r, err := http.Get(location.String())
		tests.Assert(t, err == nil)
		tests.Assert(t, r.StatusCode == http.StatusOK)
		if r.Header.Get("X-Pending") == "true" {
			time.Sleep(time.Millisecond * 10)
			continue
		} else {
			err = utils.GetJsonFromResponse(r, &info)
			tests.Assert(t, err == nil)
			break
		}
	}
}
