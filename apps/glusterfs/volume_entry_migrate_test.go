package glusterfs

import (
	"fmt"
	"os"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/heketi/heketi/executors"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/tests"
)

func createSampleDistributeOnlyVolumeEntry(size int) *VolumeEntry {
	req := &api.VolumeCreateRequest{}
	req.Size = size
	req.Durability.Type = api.DurabilityDistributeOnly

	v := NewVolumeEntryFromRequest(req)

	return v
}

func TestReplaceBrickInReplicatedVolume(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	app := NewTestApp(tmpfile)
	defer app.Close()

	// Create a cluster in the database
	err := setupSampleDbWithTopology(app,
		1,      // clusters
		4,      // nodes_per_cluster
		1,      // devices_per_node,
		500*GB, // disksize)
	)
	tests.Assert(t, err == nil)

	v := createSampleReplicaVolumeEntry(100, 3)

	err = v.Create(app.db, app.executor)
	tests.Assert(t, err == nil, err)
	var brickNames []string
	var be *BrickEntry
	err = app.db.View(func(tx *bolt.Tx) error {

		for _, brick := range v.Bricks {
			be, err = NewBrickEntryFromId(tx, brick)
			if err != nil {
				return err
			}
			ne, err := NewNodeEntryFromId(tx, be.Info.NodeId)
			if err != nil {
				return err
			}
			brickName := fmt.Sprintf("%v:%v", ne.Info.Hostnames.Storage[0], be.Info.Path)
			brickNames = append(brickNames, brickName)
		}
		return nil
	})
	app.xo.MockVolumeInfo = func(host string, volume string) (*executors.Volume, error) {
		var bricks []executors.Brick
		brick := executors.Brick{Name: brickNames[0]}
		bricks = append(bricks, brick)
		brick = executors.Brick{Name: brickNames[1]}
		bricks = append(bricks, brick)
		brick = executors.Brick{Name: brickNames[2]}
		bricks = append(bricks, brick)
		Bricks := executors.Bricks{
			BrickList: bricks,
		}
		b := &executors.Volume{
			Bricks: Bricks,
		}
		return b, nil
	}
	app.xo.MockHealInfo = func(host string, volume string) (*executors.HealInfo, error) {
		var bricks executors.HealInfoBricks
		brick := executors.BrickHealStatus{Name: brickNames[0],
			NumberOfEntries: "0"}
		bricks.BrickList = append(bricks.BrickList, brick)
		brick = executors.BrickHealStatus{Name: brickNames[1],
			NumberOfEntries: "0"}
		bricks.BrickList = append(bricks.BrickList, brick)
		brick = executors.BrickHealStatus{Name: brickNames[2],
			NumberOfEntries: "0"}
		bricks.BrickList = append(bricks.BrickList, brick)
		h := &executors.HealInfo{
			Bricks: bricks,
		}
		return h, nil
	}
	brickId := be.Id()
	err = v.replaceBrickInVolumeExtended(app.db, app.executor, brickId)
	tests.Assert(t, err == nil, err)

	oldNode := be.Info.NodeId
	brickOnOldNode := false
	oldBrickIdExists := false

	err = app.db.View(func(tx *bolt.Tx) error {

		for _, brick := range v.Bricks {
			be, err = NewBrickEntryFromId(tx, brick)
			if err != nil {
				return err
			}
			ne, err := NewNodeEntryFromId(tx, be.Info.NodeId)
			if err != nil {
				return err
			}
			if ne.Info.Id == oldNode {
				brickOnOldNode = true
			}
			if be.Info.Id == brickId {
				oldBrickIdExists = true
			}
		}
		return nil
	})

	tests.Assert(t, !brickOnOldNode, "brick found on oldNode")
	tests.Assert(t, !oldBrickIdExists, "old Brick not deleted")
}

func TestReplaceBrickInDistributeOnlyVolume(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	app := NewTestApp(tmpfile)
	defer app.Close()

	// Create a cluster in the database
	err := setupSampleDbWithTopology(app,
		1,      // clusters
		4,      // nodes_per_cluster
		1,      // devices_per_node,
		500*GB, // disksize)
	)
	tests.Assert(t, err == nil)

	v := createSampleDistributeOnlyVolumeEntry(100)

	err = v.Create(app.db, app.executor)
	tests.Assert(t, err == nil, err)
	var brickNames []string
	var be *BrickEntry
	err = app.db.View(func(tx *bolt.Tx) error {

		for _, brick := range v.Bricks {
			be, err = NewBrickEntryFromId(tx, brick)
			if err != nil {
				return err
			}
			ne, err := NewNodeEntryFromId(tx, be.Info.NodeId)
			if err != nil {
				return err
			}
			brickName := fmt.Sprintf("%v:%v", ne.Info.Hostnames.Storage[0], be.Info.Path)
			brickNames = append(brickNames, brickName)
		}
		return nil
	})
	app.xo.MockVolumeInfo = func(host string, volume string) (*executors.Volume, error) {
		var bricks []executors.Brick
		brick := executors.Brick{Name: brickNames[0]}
		bricks = append(bricks, brick)
		Bricks := executors.Bricks{
			BrickList: bricks,
		}
		b := &executors.Volume{
			Bricks: Bricks,
		}
		return b, nil
	}
	app.xo.MockHealInfo = func(host string, volume string) (*executors.HealInfo, error) {
		var bricks executors.HealInfoBricks
		brick := executors.BrickHealStatus{Name: brickNames[0],
			NumberOfEntries: "0"}
		bricks.BrickList = append(bricks.BrickList, brick)
		h := &executors.HealInfo{
			Bricks: bricks,
		}
		return h, nil
	}
	brickId := be.Id()
	err = v.replaceBrickInVolumeExtended(app.db, app.executor, brickId)
	tests.Assert(t, err == nil, err)

	oldNode := be.Info.NodeId
	brickOnOldNode := false
	oldBrickIdExists := false

	err = app.db.View(func(tx *bolt.Tx) error {

		for _, brick := range v.Bricks {
			be, err = NewBrickEntryFromId(tx, brick)
			if err != nil {
				return err
			}
			ne, err := NewNodeEntryFromId(tx, be.Info.NodeId)
			if err != nil {
				return err
			}
			if ne.Info.Id == oldNode {
				brickOnOldNode = true
			}
			if be.Info.Id == brickId {
				oldBrickIdExists = true
			}
		}
		return nil
	})

	tests.Assert(t, !brickOnOldNode, "brick found on oldNode")
	tests.Assert(t, !oldBrickIdExists, "old Brick not deleted")
}

func TestReplaceBrickInDistributeOnlyVolumeNotAllowed(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	app := NewTestApp(tmpfile)
	defer app.Close()

	// Create a cluster in the database
	err := setupSampleDbWithTopology(app,
		1,      // clusters
		1,      // nodes_per_cluster
		1,      // devices_per_node,
		500*GB, // disksize)
	)
	tests.Assert(t, err == nil)

	v := createSampleDistributeOnlyVolumeEntry(100)

	err = v.Create(app.db, app.executor)
	tests.Assert(t, err == nil, err)
	var brickNames []string
	var be *BrickEntry
	err = app.db.View(func(tx *bolt.Tx) error {

		for _, brick := range v.Bricks {
			be, err = NewBrickEntryFromId(tx, brick)
			if err != nil {
				return err
			}
			ne, err := NewNodeEntryFromId(tx, be.Info.NodeId)
			if err != nil {
				return err
			}
			brickName := fmt.Sprintf("%v:%v", ne.Info.Hostnames.Storage[0], be.Info.Path)
			brickNames = append(brickNames, brickName)
		}
		return nil
	})
	app.xo.MockVolumeInfo = func(host string, volume string) (*executors.Volume, error) {
		var bricks []executors.Brick
		brick := executors.Brick{Name: brickNames[0]}
		bricks = append(bricks, brick)
		Bricks := executors.Bricks{
			BrickList: bricks,
		}
		b := &executors.Volume{
			Bricks: Bricks,
		}
		return b, nil
	}
	app.xo.MockHealInfo = func(host string, volume string) (*executors.HealInfo, error) {
		var bricks executors.HealInfoBricks
		brick := executors.BrickHealStatus{Name: brickNames[0],
			NumberOfEntries: "0"}
		bricks.BrickList = append(bricks.BrickList, brick)
		h := &executors.HealInfo{
			Bricks: bricks,
		}
		return h, nil
	}
	brickId := be.Id()
	err = v.replaceBrickInVolumeExtended(app.db, app.executor, brickId)
	tests.Assert(t, err != nil, err)

	oldNode := be.Info.NodeId
	brickOnOldNode := false
	oldBrickIdExists := false

	err = app.db.View(func(tx *bolt.Tx) error {

		for _, brick := range v.Bricks {
			be, err = NewBrickEntryFromId(tx, brick)
			if err != nil {
				return err
			}
			ne, err := NewNodeEntryFromId(tx, be.Info.NodeId)
			if err != nil {
				return err
			}
			if ne.Info.Id == oldNode {
				brickOnOldNode = true
			}
			if be.Info.Id == brickId {
				oldBrickIdExists = true
			}
		}
		return nil
	})

	tests.Assert(t, brickOnOldNode, "brick found on oldNode")
	tests.Assert(t, oldBrickIdExists, "old Brick not deleted")
}

func TestReplaceBrickInDistributeOnlyVolumeOnOneNode(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	app := NewTestApp(tmpfile)
	defer app.Close()

	// Create a cluster in the database
	err := setupSampleDbWithTopology(app,
		1,      // clusters
		1,      // nodes_per_cluster
		2,      // devices_per_node,
		500*GB, // disksize)
	)
	tests.Assert(t, err == nil)

	v := createSampleDistributeOnlyVolumeEntry(100)

	err = v.Create(app.db, app.executor)
	tests.Assert(t, err == nil, err)
	var brickNames []string
	var be *BrickEntry
	err = app.db.View(func(tx *bolt.Tx) error {

		for _, brick := range v.Bricks {
			be, err = NewBrickEntryFromId(tx, brick)
			if err != nil {
				return err
			}
			ne, err := NewNodeEntryFromId(tx, be.Info.NodeId)
			if err != nil {
				return err
			}
			brickName := fmt.Sprintf("%v:%v", ne.Info.Hostnames.Storage[0], be.Info.Path)
			brickNames = append(brickNames, brickName)
		}
		return nil
	})
	app.xo.MockVolumeInfo = func(host string, volume string) (*executors.Volume, error) {
		var bricks []executors.Brick
		brick := executors.Brick{Name: brickNames[0]}
		bricks = append(bricks, brick)
		Bricks := executors.Bricks{
			BrickList: bricks,
		}
		b := &executors.Volume{
			Bricks: Bricks,
		}
		return b, nil
	}
	app.xo.MockHealInfo = func(host string, volume string) (*executors.HealInfo, error) {
		var bricks executors.HealInfoBricks
		brick := executors.BrickHealStatus{Name: brickNames[0],
			NumberOfEntries: "0"}
		bricks.BrickList = append(bricks.BrickList, brick)
		h := &executors.HealInfo{
			Bricks: bricks,
		}
		return h, nil
	}
	brickId := be.Id()
	err = v.replaceBrickInVolumeExtended(app.db, app.executor, brickId)
	tests.Assert(t, err == nil, err)

	oldNode := be.Info.NodeId
	brickOnOldNode := false
	oldBrickIdExists := false

	err = app.db.View(func(tx *bolt.Tx) error {

		for _, brick := range v.Bricks {
			be, err = NewBrickEntryFromId(tx, brick)
			if err != nil {
				return err
			}
			ne, err := NewNodeEntryFromId(tx, be.Info.NodeId)
			if err != nil {
				return err
			}
			if ne.Info.Id == oldNode {
				brickOnOldNode = true
			}
			if be.Info.Id == brickId {
				oldBrickIdExists = true
			}
		}
		return nil
	})

	tests.Assert(t, !brickOnOldNode, "brick found on oldNode")
	tests.Assert(t, !oldBrickIdExists, "old Brick not deleted")
}

func TestReplaceBrickInDistributeOnlyVolumeOnOneOfflineNode(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	app := NewTestApp(tmpfile)
	defer app.Close()

	// Create a cluster in the database
	err := setupSampleDbWithTopology(app,
		1,      // clusters
		1,      // nodes_per_cluster
		2,      // devices_per_node,
		500*GB, // disksize)
	)
	tests.Assert(t, err == nil)

	v := createSampleDistributeOnlyVolumeEntry(100)

	err = v.Create(app.db, app.executor)
	tests.Assert(t, err == nil, err)
	var brickNames []string
	var be *BrickEntry
	err = app.db.View(func(tx *bolt.Tx) error {

		for _, brick := range v.Bricks {
			be, err = NewBrickEntryFromId(tx, brick)
			if err != nil {
				return err
			}
			ne, err := NewNodeEntryFromId(tx, be.Info.NodeId)
			if err != nil {
				return err
			}
			brickName := fmt.Sprintf("%v:%v", ne.Info.Hostnames.Storage[0], be.Info.Path)
			brickNames = append(brickNames, brickName)
		}
		return nil
	})
	app.xo.MockVolumeInfo = func(host string, volume string) (*executors.Volume, error) {
		var bricks []executors.Brick
		brick := executors.Brick{Name: brickNames[0]}
		bricks = append(bricks, brick)
		Bricks := executors.Bricks{
			BrickList: bricks,
		}
		b := &executors.Volume{
			Bricks: Bricks,
		}
		return b, nil
	}
	app.xo.MockHealInfo = func(host string, volume string) (*executors.HealInfo, error) {
		var bricks executors.HealInfoBricks
		brick := executors.BrickHealStatus{Name: brickNames[0],
			NumberOfEntries: "0"}
		bricks.BrickList = append(bricks.BrickList, brick)
		h := &executors.HealInfo{
			Bricks: bricks,
		}
		return h, nil
	}

	// mark a node as offline
	var clusterId string
	app.db.Update(func(tx *bolt.Tx) error {
		cids, err := ClusterList(tx)
		tests.Assert(t, err == nil, "expected err == nil, got:", err)

		clusterId = cids[0]
		c, err := NewClusterEntryFromId(tx, clusterId)
		tests.Assert(t, err == nil, "expected err == nil, got:", err)

		// set the first node offline
		n, err := NewNodeEntryFromId(tx, c.Info.Nodes[0])
		tests.Assert(t, err == nil, "expected err == nil, got:", err)
		n.State = api.EntryStateOffline
		err = n.Save(tx)
		tests.Assert(t, err == nil, "expected err == nil, got:", err)

		return nil
	})

	brickId := be.Id()
	err = v.replaceBrickInVolumeExtended(app.db, app.executor, brickId)
	tests.Assert(t, err != nil, err)
}

func TestRemoveBrickFromReplicatedVolumeReducingReplicaCount(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	app := NewTestApp(tmpfile)
	defer app.Close()

	// Create a cluster in the database
	err := setupSampleDbWithTopology(app,
		1,      // clusters
		4,      // nodes_per_cluster
		1,      // devices_per_node,
		500*GB, // disksize)
	)
	tests.Assert(t, err == nil)

	v := createSampleReplicaVolumeEntry(100, 3)

	err = v.Create(app.db, app.executor)
	tests.Assert(t, err == nil, err)
	var brickNames []string
	var be *BrickEntry
	err = app.db.View(func(tx *bolt.Tx) error {

		for _, brick := range v.Bricks {
			be, err = NewBrickEntryFromId(tx, brick)
			if err != nil {
				return err
			}
			ne, err := NewNodeEntryFromId(tx, be.Info.NodeId)
			if err != nil {
				return err
			}
			brickName := fmt.Sprintf("%v:%v", ne.Info.Hostnames.Storage[0], be.Info.Path)
			brickNames = append(brickNames, brickName)
		}
		return nil
	})

	brickId := be.Id()
	err = v.removeBrickFromVolume(app.db, app.executor, brickId)
	tests.Assert(t, err == nil, err)

	oldNode := be.Info.NodeId
	brickOnOldNode := false
	oldBrickIdExists := false

	err = app.db.View(func(tx *bolt.Tx) error {
		v, err = NewVolumeEntryFromId(tx, v.Info.Id)
		for _, brick := range v.Bricks {
			be, err = NewBrickEntryFromId(tx, brick)
			if err != nil {
				return err
			}
			ne, err := NewNodeEntryFromId(tx, be.Info.NodeId)
			if err != nil {
				return err
			}
			if ne.Info.Id == oldNode {
				brickOnOldNode = true
			}
			if be.Info.Id == brickId {
				oldBrickIdExists = true
			}
		}
		return nil
	})

	tests.Assert(t, v.Info.Durability.Replicate.Replica == 2, "expected Replica == 2, got:", v.Info.Durability.Replicate.Replica)
	tests.Assert(t, v.Durability.BricksInSet() == 2, "expected BricksInSet == 2, got:", v.Durability.BricksInSet())
	tests.Assert(t, !brickOnOldNode, "brick found on oldNode")
	tests.Assert(t, !oldBrickIdExists, "old Brick not deleted")
}
