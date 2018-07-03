package glusterfs

import (
	"fmt"

	"github.com/heketi/heketi/executors"
	wdb "github.com/heketi/heketi/pkg/db"

	"github.com/boltdb/bolt"
)

// VolumeMigrateOperation implements the operation functions used to migrate volume data from node.
type VolumeMigrateOperation struct {
	OperationManager
	noRetriesOperation
	vol    *VolumeEntry
	nodeID string
}

func NewVolumeMigrateOperation(vol *VolumeEntry, db wdb.DB, nodeID string) *VolumeMigrateOperation {
	return &VolumeMigrateOperation{
		OperationManager: OperationManager{
			db: db,
			op: NewPendingOperationEntry(NEW_ID),
		},
		vol:    vol,
		nodeID: nodeID,
	}
}

func (vm *VolumeMigrateOperation) Label() string {
	return "Migrate Volume from Node"
}

func (vm *VolumeMigrateOperation) ResourceUrl() string {
	return fmt.Sprintf("/volumes/%v", vm.vol.Info.Id)
}

func (vm *VolumeMigrateOperation) Build() error {
	return vm.db.Update(func(tx *bolt.Tx) error {
		vm.op.RecordMigrateVolume(vm.vol)
		if err := vm.op.Save(tx); err != nil {
			return err
		}
		return nil
	})
}

func (vm *VolumeMigrateOperation) Exec(executor executors.Executor) error {
	return vm.vol.migrateBricksFromNode(vm.db, executor, vm.nodeID)
}

func (vm *VolumeMigrateOperation) Rollback(executor executors.Executor) error {
	return vm.db.Update(func(tx *bolt.Tx) error {
		vm.op.FinalizeVolumeMigrate(vm.vol)
		if err := vm.vol.Save(tx); err != nil {
			return err
		}
		vm.op.Delete(tx)
		return nil
	})
}

func (vm *VolumeMigrateOperation) Finalize() error {
	return vm.db.Update(func(tx *bolt.Tx) error {
		vm.op.FinalizeVolumeMigrate(vm.vol)
		if err := vm.vol.Save(tx); err != nil {
			return err
		}
		vm.op.Delete(tx)
		return nil
	})
}
