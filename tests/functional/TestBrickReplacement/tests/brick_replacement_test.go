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
	"strings"
	"testing"

	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/tests"
)

func TestBrickReplacementInDistributedOnlyVolume(t *testing.T) {
	// We should be able to replace brick in distributed only volume
	// if it is possible to allocate brick replacement on another node

	// Setup the VM storage topology
	teardownCluster(t)
	setupCluster(t, 3, 2)
	defer teardownCluster(t)

	// We have 2 disks of 500GB on every node
	// Total space per node is 1TB
	// We have 3 Nodes, so total space is 3TB

	// vol: 300 ==> 1 replica set
	volReq := &api.VolumeCreateRequest{}
	volReq.Size = 300
	volReq.Durability.Type = api.DurabilityDistributeOnly
	vol, err := heketi.VolumeCreate(volReq)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)

	// Check there is only one
	volumes, err := heketi.VolumeList()
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
	tests.Assert(t, len(volumes.Volumes) == 1)

	deviceOccurrence := make(map[string]int)
	maxBricksPerDevice := 0
	var deviceToRemove string
	for _, brick := range vol.Bricks {
		deviceOccurrence[brick.DeviceId]++
		if deviceOccurrence[brick.DeviceId] > maxBricksPerDevice {
			maxBricksPerDevice = deviceOccurrence[brick.DeviceId]
			deviceToRemove = brick.DeviceId
		}
	}

	for device, _ := range deviceOccurrence {
		logger.Info("Key: %v , Value: %v", device, deviceOccurrence[device])
	}

	// if this fails, it's a problem with the test ...
	tests.Assert(t, maxBricksPerDevice == 1, "Problem: failed to produce a disk with multiple bricks from one volume!")

	stateReq := &api.StateRequest{}
	stateReq.State = api.EntryStateOffline
	err = heketi.DeviceState(deviceToRemove, stateReq)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)

	// Set device state to failed triggers device removing, which triggers removing bricks from device
	stateReq = &api.StateRequest{}
	stateReq.State = api.EntryStateFailed
	err = heketi.DeviceState(deviceToRemove, stateReq)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)

	logger.Info("%v", vol)
	// Delete volumes
	err = heketi.VolumeDelete(vol.Id)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
}

func TestBrickReplacementInDistributedOnlyVolumeNotAllowed(t *testing.T) {
	// We should not be able to replace brick in distributed only volume
	// if it is not possible to allocate brick replacement on another node

	// Setup the VM storage topology
	teardownCluster(t)
	setupCluster(t, 1, 1)
	defer teardownCluster(t)

	// We have 1 disks of 500GB on every node
	// Total space per node is 500GB
	// We have 1 Node, so total space is 500GB
	// No device for brick replacement

	// vol: 300 ==> 1 replica set
	volReq := &api.VolumeCreateRequest{}
	volReq.Size = 300
	volReq.Durability.Type = api.DurabilityDistributeOnly
	vol, err := heketi.VolumeCreate(volReq)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)

	// Check there is only one
	volumes, err := heketi.VolumeList()
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
	tests.Assert(t, len(volumes.Volumes) == 1)

	deviceOccurrence := make(map[string]int)
	maxBricksPerDevice := 0
	var deviceToRemove string
	for _, brick := range vol.Bricks {
		deviceOccurrence[brick.DeviceId]++
		if deviceOccurrence[brick.DeviceId] > maxBricksPerDevice {
			maxBricksPerDevice = deviceOccurrence[brick.DeviceId]
			deviceToRemove = brick.DeviceId
		}
	}

	for device, _ := range deviceOccurrence {
		logger.Info("Key: %v , Value: %v", device, deviceOccurrence[device])
	}

	// if this fails, it's a problem with the test ...
	tests.Assert(t, maxBricksPerDevice == 1, "Problem: failed to produce a disk with multiple bricks from one volume!")

	stateReq := &api.StateRequest{}
	stateReq.State = api.EntryStateOffline
	err = heketi.DeviceState(deviceToRemove, stateReq)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)

	// Set device state to failed triggers device removing, which triggers removing bricks from device
	stateReq = &api.StateRequest{}
	stateReq.State = api.EntryStateFailed
	err = heketi.DeviceState(deviceToRemove, stateReq)
	tests.Assert(t, err != nil, "expected err != nil, got:", err)
	tests.Assert(t, strings.Contains(err.Error(), "remove brick is not allowed"),
		"expected error contains \"remove brick is not allowed\", got:", err.Error())

	logger.Info("%v", vol)
	// Delete volumes
	err = heketi.VolumeDelete(vol.Id)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
}

func TestBrickReplacementInReplicatedVolumeReducingReplicaCount(t *testing.T) {
	// We should be able to replace brick in replicated volume
	// if it is not possible to allocate brick replacement on another node by reducing replica count

	// Setup the VM storage topology
	teardownCluster(t)
	setupCluster(t, 3, 1)
	defer teardownCluster(t)

	// We have 1 disks of 500GB on every node
	// Total space per node is 500GB
	// We have 3 Nodes, so total space is 1.5TB

	// vol: 300 ==> 3 replica sets of 300 each on each node

	volReq := &api.VolumeCreateRequest{}
	volReq.Size = 300
	volReq.Durability.Type = api.DurabilityReplicate
	volReq.Durability.Replicate.Replica = 3
	vol, err := heketi.VolumeCreate(volReq)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)

	// Check there is only one
	volumes, err := heketi.VolumeList()
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
	tests.Assert(t, len(volumes.Volumes) == 1)

	deviceOccurrence := make(map[string]int)
	maxBricksPerDevice := 0
	var deviceToRemove string
	for _, brick := range vol.Bricks {
		deviceOccurrence[brick.DeviceId]++
		if deviceOccurrence[brick.DeviceId] > maxBricksPerDevice {
			maxBricksPerDevice = deviceOccurrence[brick.DeviceId]
			deviceToRemove = brick.DeviceId
		}
	}

	for device, _ := range deviceOccurrence {
		logger.Info("Key: %v , Value: %v", device, deviceOccurrence[device])
	}

	// if this fails, it's a problem with the test ...
	tests.Assert(t, maxBricksPerDevice == 1, "Problem: failed to produce a disk with multiple bricks from one volume!")

	stateReq := &api.StateRequest{}
	stateReq.State = api.EntryStateOffline
	err = heketi.DeviceState(deviceToRemove, stateReq)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)

	stateReq = &api.StateRequest{}
	stateReq.State = api.EntryStateFailed
	err = heketi.DeviceState(deviceToRemove, stateReq)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)

	vol, err = heketi.VolumeInfo(vol.Id)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
	tests.Assert(t, vol.Durability.Replicate.Replica == 2, "expected Replica == 2, got:", vol.Durability.Replicate.Replica)

	logger.Info("%v", vol)
	// Delete volumes
	err = heketi.VolumeDelete(vol.Id)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
}
