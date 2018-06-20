// +build functional

//
// Copyright (c) 2015 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//
package functional

import (
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/heketi/heketi/client/api/go-client"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/utils"
	"github.com/heketi/tests"
)

var (
	// The heketi server must be running on the host
	heketiUrl = "http://localhost:8080"

	// VMs
	storage0    = "192.168.10.100"
	storage1    = "192.168.10.101"
	storage2    = "192.168.10.102"
	storage3    = "192.168.10.103"
	portNum     = "22"
	storage0ssh = storage0 + ":" + portNum
	storage1ssh = storage1 + ":" + portNum
	storage2ssh = storage2 + ":" + portNum
	storage3ssh = storage3 + ":" + portNum

	// Heketi client
	heketi = client.NewClientNoAuth(heketiUrl)
	logger = utils.NewLogger("[test]", utils.LEVEL_DEBUG)

	// Storage systems
	storagevms = []string{
		storage0,
		storage1,
		storage2,
		storage3,
	}

	// Disks on each system
	disks = []string{
		"/dev/vdb",
		"/dev/vdc",
		"/dev/vdd",
		"/dev/vde",

		"/dev/vdf",
		"/dev/vdg",
		"/dev/vdh",
		"/dev/vdi",
	}
)

func setupCluster(t *testing.T, numNodes int, numDisks int) {
	tests.Assert(t, heketi != nil)

	// Get ssh port first, we need it to create
	// storageXssh variables
	env := os.Getenv("HEKETI_TEST_STORAGEPORT")
	if "" != env {
		portNum = env
	}

	env = os.Getenv("HEKETI_TEST_STORAGE0")
	if "" != env {
		storage0 = env
		storage0ssh = storage0 + ":" + portNum
	}
	env = os.Getenv("HEKETI_TEST_STORAGE1")
	if "" != env {
		storage1 = env
		storage1ssh = storage1 + ":" + portNum
	}
	env = os.Getenv("HEKETI_TEST_STORAGE2")
	if "" != env {
		storage2 = env
		storage2ssh = storage2 + ":" + portNum
	}
	env = os.Getenv("HEKETI_TEST_STORAGE3")
	if "" != env {
		storage3 = env
		storage3ssh = storage3 + ":" + portNum
	}
	// Storage systems
	storagevms = []string{
		storage0,
		storage1,
		storage2,
		storage3,
	}
	// Create a cluster
	cluster_req := &api.ClusterCreateRequest{
		ClusterFlags: api.ClusterFlags{
			Block: true,
			File:  true,
		},
	}

	cluster, err := heketi.ClusterCreate(cluster_req)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)

	// hardcoded limits from the lists above
	// possible TODO: generalize
	tests.Assert(t, numNodes <= 4)
	tests.Assert(t, numDisks <= 8)

	// Add nodes
	for index, hostname := range storagevms[:numNodes] {
		nodeReq := &api.NodeAddRequest{}
		nodeReq.ClusterId = cluster.Id
		nodeReq.Hostnames.Manage = []string{hostname}
		nodeReq.Hostnames.Storage = []string{hostname}
		nodeReq.Zone = index%2 + 1

		node, err := heketi.NodeAdd(nodeReq)
		tests.Assert(t, err == nil, "expected err == nil, got:", err)

		// Add devices
		sg := utils.NewStatusGroup()
		for _, disk := range disks[:numDisks] {
			sg.Add(1)
			go func(d string) {
				defer sg.Done()

				driveReq := &api.DeviceAddRequest{}
				driveReq.Name = d
				driveReq.NodeId = node.Id

				err := heketi.DeviceAdd(driveReq)
				sg.Err(err)
			}(disk)
		}

		err = sg.Result()
		tests.Assert(t, err == nil, "expected err == nil, got:", err)
	}
}

func dbStateDump(t *testing.T) {
	if t.Failed() {
		fmt.Println("~~~~~ dumping db state prior to teardown ~~~~~")
		dump, err := heketi.DbDump()
		if err != nil {
			fmt.Printf("Unable to get db dump: %v\n", err)
		} else {
			fmt.Printf("\n%v\n", dump)
		}
		fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	}
}

func teardownCluster(t *testing.T) {
	dbStateDump(t)

	clusters, err := heketi.ClusterList()
	tests.Assert(t, err == nil, err)

	for _, cluster := range clusters.Clusters {

		clusterInfo, err := heketi.ClusterInfo(cluster)
		tests.Assert(t, err == nil, "expected err == nil, got:", err)

		// Delete volumes in this cluster
		for _, volume := range clusterInfo.Volumes {
			err := heketi.VolumeDelete(volume)
			tests.Assert(t, err == nil, "expected err == nil, got:", err)
		}

		// Delete nodes
		for _, node := range clusterInfo.Nodes {

			// Get node information
			nodeInfo, err := heketi.NodeInfo(node)
			tests.Assert(t, err == nil, "expected err == nil, got:", err)

			// Delete each device
			sg := utils.NewStatusGroup()
			for _, device := range nodeInfo.DevicesInfo {
				sg.Add(1)
				go func(id string) {
					defer sg.Done()

					stateReq := &api.StateRequest{}
					stateReq.State = api.EntryStateOffline
					err := heketi.DeviceState(id, stateReq)
					if err != nil {
						sg.Err(err)
						return
					}

					stateReq.State = api.EntryStateFailed
					err = heketi.DeviceState(id, stateReq)
					if err != nil {
						sg.Err(err)
						return
					}

					err = heketi.DeviceDelete(id)
					sg.Err(err)

				}(device.Id)
			}
			err = sg.Result()
			tests.Assert(t, err == nil, "expected err == nil, got:", err)

			// Delete node
			err = heketi.NodeDelete(node)
			tests.Assert(t, err == nil, "expected err == nil, got:", err)
		}

		// Delete cluster
		err = heketi.ClusterDelete(cluster)
		tests.Assert(t, err == nil, "expected err == nil, got:", err)
	}
}

func TestConnection(t *testing.T) {
	r, err := http.Get(heketiUrl + "/hello")
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
	tests.Assert(t, r.StatusCode == http.StatusOK)
}
