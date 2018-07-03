package glusterfs

import (
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/heketi/heketi/executors"
	wdb "github.com/heketi/heketi/pkg/db"
	"github.com/heketi/heketi/pkg/glusterfs/api"
)


func (v *VolumeEntry) migrateBricksFromNode(db wdb.DB, executor executors.Executor) (e error) {

	errBrickWithEmptyPath := fmt.Errorf("brick has no path")

	for _, brickId := range v.Bricks {
		var brickEntry *BrickEntry
		var volumeEntry *VolumeEntry
		err := db.View(func(tx *bolt.Tx) error {
			var err error
			brickEntry, err = NewBrickEntryFromId(tx, brickId)
			if err != nil {
				return err
			}
			// Handle the special error case when brick has no path
			// we skip the brick and continue
			if brickEntry.Info.Path == "" {
				return errBrickWithEmptyPath
			}
			volumeEntry, err = NewVolumeEntryFromId(tx, brickEntry.Info.VolumeId)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			if err == errBrickWithEmptyPath {
				logger.Warning("Skipping brick with empty path, brickID: %v, volumeID: %v, error: %v", brickEntry.Info.Id, brickEntry.Info.VolumeId, err)
				continue
			}
			return err
		}
		logger.Info("Replacing brick %v on device %v on node %v", brickEntry.Id(), brickEntry., d.NodeId)
		err = volumeEntry.replaceBrickInVolume(db, executor, brickEntry.Id())
		if err == ErrNoReplacement {
			err = volumeEntry.removeBrickFromVolume(db, executor, brickEntry.Id())
		}
		if err != nil {
			return logger.Err(fmt.Errorf("Failed to remove device, error: %v", err))
		}
	}
	return nil
}

func (v *VolumeEntry) replaceBrick(db wdb.DB, executor executors.Executor, oldBrickId string) (e error) {
	ri, node, err := v.prepForBrickReplacement(
		db, executor, oldBrickId)
	if err != nil {
		return err
	}
	// unpack the struct so we don't have to mess w/ the lower half of
	// this function
	oldBrickEntry := ri.oldBrickEntry
	oldDeviceEntry := ri.oldDeviceEntry
	oldBrickNodeEntry := ri.oldBrickNodeEntry

	newBrickEntry, newDeviceEntry, err := v.allocBrickReplacement(
		db, oldBrickEntry, oldDeviceEntry, ri.bs, ri.index)
	if err != nil {
		return err
	}

	defer func() {
		if e != nil {
			db.Update(func(tx *bolt.Tx) error {
				newDeviceEntry, err = NewDeviceEntryFromId(tx, newBrickEntry.Info.DeviceId)
				if err != nil {
					return err
				}
				newDeviceEntry.StorageFree(newBrickEntry.TotalSize())
				newDeviceEntry.Save(tx)
				return nil
			})
		}
	}()

	var newBrickNodeEntry *NodeEntry
	err = db.View(func(tx *bolt.Tx) error {
		newBrickNodeEntry, err = NewNodeEntryFromId(tx, newBrickEntry.Info.NodeId)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	brickEntries := []*BrickEntry{newBrickEntry}
	err = CreateBricks(db, executor, brickEntries)
	if err != nil {
		return err
	}

	defer func() {
		if e != nil {
			DestroyBricks(db, executor, brickEntries)
		}
	}()

	var oldBrick executors.BrickInfo
	var newBrick executors.BrickInfo

	oldBrick.Path = oldBrickEntry.Info.Path
	oldBrick.Host = oldBrickNodeEntry.StorageHostName()
	newBrick.Path = newBrickEntry.Info.Path
	newBrick.Host = newBrickNodeEntry.StorageHostName()

	err = executor.VolumeReplaceBrick(node, v.Info.Name, &oldBrick, &newBrick)
	if err != nil {
		return err
	}

	// After this point we should not call any defer func()
	// We don't have a *revert* of replace brick operation

	spaceReclaimed, err := oldBrickEntry.Destroy(db, executor)
	if err != nil {
		logger.LogError("Error destroying old brick: %v", err)
	}

	// We must read entries from db again as state on disk might
	// have changed

	err = db.Update(func(tx *bolt.Tx) error {
		err = newBrickEntry.Save(tx)
		if err != nil {
			return err
		}
		reReadNewDeviceEntry, err := NewDeviceEntryFromId(tx, newBrickEntry.Info.DeviceId)
		if err != nil {
			return err
		}
		reReadNewDeviceEntry.BrickAdd(newBrickEntry.Id())
		err = reReadNewDeviceEntry.Save(tx)
		if err != nil {
			return err
		}
		if spaceReclaimed {
			oldDevice2, err := NewDeviceEntryFromId(tx, oldBrickEntry.Info.DeviceId)
			if err != nil {
				return err
			}
			oldDevice2.StorageFree(oldBrickEntry.TotalSize())
			err = oldDevice2.Save(tx)
			if err != nil {
				return err
			}
		}

		reReadVolEntry, err := NewVolumeEntryFromId(tx, newBrickEntry.Info.VolumeId)
		if err != nil {
			return err
		}
		reReadVolEntry.BrickAdd(newBrickEntry.Id())
		err = reReadVolEntry.removeBrickFromDb(tx, oldBrickEntry)
		if err != nil {
			return err
		}
		err = reReadVolEntry.Save(tx)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logger.Err(err)
	}

	logger.Info("replaced brick:%v on node:%v at path:%v with brick:%v on node:%v at path:%v",
		oldBrickEntry.Id(), oldBrickEntry.Info.NodeId, oldBrickEntry.Info.Path,
		newBrickEntry.Id(), newBrickEntry.Info.NodeId, newBrickEntry.Info.Path)
	return nil
}

func (v *VolumeEntry) removeBrick(db wdb.DB, executor executors.Executor, oldBrickId string) (e error) {
	if api.DurabilityReplicate != v.Info.Durability.Type {
		return fmt.Errorf("remove brick is not allowed for volume durability type %v", v.Info.Durability.Type)
	}

	if v.Info.Durability.Replicate.Replica <= 1 {
		return fmt.Errorf("replica count can't be reduced lower than %d", 1)
	}

	bi, node, err := v.prepForBrickRemoval(db, executor, oldBrickId)
	if err != nil {
		return err
	}

	brickEntry := bi.brickEntry
	brickNodeEntry := bi.brickNodeEntry

	var oldBrick executors.BrickInfo
	oldBrick.Path = brickEntry.Info.Path
	oldBrick.Host = brickNodeEntry.StorageHostName()

	// Remove brick reducing replica count
	replica := v.Info.Durability.Replicate.Replica - 1
	err = executor.VolumeRemoveBrick(node, v.Info.Name, &oldBrick, replica)
	if err != nil {
		return err
	}

	spaceReclaimed, err := brickEntry.Destroy(db, executor)
	if err != nil {
		logger.LogError("Error destroying brick: %v", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		if spaceReclaimed {
			reReadDeviceEntry, err := NewDeviceEntryFromId(tx, brickEntry.Info.DeviceId)
			if err != nil {
				return err
			}
			reReadDeviceEntry.StorageFree(brickEntry.TotalSize())
			err = reReadDeviceEntry.Save(tx)
			if err != nil {
				return err
			}
		}

		reReadVolEntry, err := NewVolumeEntryFromId(tx, brickEntry.Info.VolumeId)
		if err != nil {
			return err
		}
		if replica > 1 {
			reReadVolEntry.Info.Durability.Replicate.Replica = replica
			reReadVolEntry.Durability = NewVolumeReplicaDurability(&reReadVolEntry.Info.Durability.Replicate)
		} else {
			reReadVolEntry.Info.Durability.Type = api.DurabilityDistributeOnly
			reReadVolEntry.Durability = NewNoneDurability()
		}
		err = reReadVolEntry.removeBrickFromDb(tx, brickEntry)
		if err != nil {
			return err
		}
		err = reReadVolEntry.Save(tx)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logger.Err(err)
	}

	logger.Info("removed brick:%v from node:%v at path:%v",
		brickEntry.Id(), brickEntry.Info.NodeId, brickEntry.Info.Path)

	return nil
}

type brickItem struct {
	brickEntry     *BrickEntry
	deviceEntry    *DeviceEntry
	brickNodeEntry *NodeEntry
}

func (v *VolumeEntry) prepForBrickRemoval(db wdb.DB,
	executor executors.Executor,
	oldBrickId string) (bi brickItem, node string, err error) {

	var oldBrickEntry *BrickEntry
	var oldDeviceEntry *DeviceEntry
	var oldBrickNodeEntry *NodeEntry

	err = db.View(func(tx *bolt.Tx) error {
		var err error
		oldBrickEntry, err = NewBrickEntryFromId(tx, oldBrickId)
		if err != nil {
			return err
		}

		oldDeviceEntry, err = NewDeviceEntryFromId(tx, oldBrickEntry.Info.DeviceId)
		if err != nil {
			return err
		}
		oldBrickNodeEntry, err = NewNodeEntryFromId(tx, oldBrickEntry.Info.NodeId)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return
	}

	node = oldBrickNodeEntry.ManageHostName()
	err = executor.GlusterdCheck(node)
	if err != nil {
		node, err = GetVerifiedManageHostname(db, executor, oldBrickNodeEntry.Info.ClusterId)
		if err != nil {
			return
		}
	}

	bi = brickItem{
		brickEntry:     oldBrickEntry,
		deviceEntry:    oldDeviceEntry,
		brickNodeEntry: oldBrickNodeEntry,
	}
	return
}

