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
	"testing"

	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/tests"
	"github.com/heketi/heketi/pkg/utils"
)

func TestNodeRemoval(t *testing.T) {
	// We should be able to remove 2 nodes from cluster with replica 3 volume
	// Volume type should be changed to Distributed only

	// Setup the VM storage topology
	teardownCluster(t)
	setupCluster(t, 3, 2)
	defer teardownCluster(t)

	// We have 2 disks of 500GB on every node
	// Total space per node is 1TB
	// We have 3 Nodes, so total space is 3TB

	// vol: 300 ==> 2 replica sets of 300 each on each node

	volReq := &api.VolumeCreateRequest{}
	volReq.Size = 300
	volReq.Durability.Type = api.DurabilityReplicate
	volReq.Durability.Replicate.Replica = 3
	vol, err := heketi.VolumeCreate(volReq)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)

	clusterList, err := heketi.ClusterList()
	tests.Assert(t, err == nil, err)

	cluster := clusterList.Clusters[0]
	clusterInfo, err := heketi.ClusterInfo(cluster)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)

	// Delete first node
	node := clusterInfo.Nodes[0]
	nodeInfo, err := heketi.NodeInfo(node)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
	err = deleteNode(nodeInfo)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)

	vol, err = heketi.VolumeInfo(vol.Id)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
	tests.Assert(t, vol.Durability.Type == api.DurabilityReplicate, "expected durability Replicate, got:", vol.Durability.Type)
	tests.Assert(t, vol.Durability.Replicate.Replica == 2, "expected Replica == 2, got:", vol.Durability.Replicate.Replica)

	// Delete second node
	node = clusterInfo.Nodes[1]
	nodeInfo, err = heketi.NodeInfo(node)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
	err = deleteNode(nodeInfo)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)

	vol, err = heketi.VolumeInfo(vol.Id)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
	tests.Assert(t, vol.Durability.Type == api.DurabilityDistributeOnly, "expected durability DistributeOnly, got:", vol.Durability.Type)

	// Delete last node
	node = clusterInfo.Nodes[2]
	nodeInfo, err = heketi.NodeInfo(node)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
	err = deleteNode(nodeInfo)
	tests.Assert(t, err != nil, "expected err != nil, got:", err)
}

func deleteNode(nodeInfo *api.NodeInfoResponse) error {
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

	return sg.Result()
}