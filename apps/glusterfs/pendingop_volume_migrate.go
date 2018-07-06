package glusterfs

const (
	OperationMigrateVolume PendingOperationType = 10 // Related to apps/glusterfs/pendingop.go:31
	OpMigrateVolume        PendingChangeType    = 20 // Related to apps/glusterfs/pendingop.go:46
)

func (p *PendingOperationEntry) RecordMigrateVolume(v *VolumeEntry) {
	p.recordChange(OpMigrateVolume, v.Info.Id)
	p.Type = OperationMigrateVolume
	v.Pending.Id = p.Id
}

func (p *PendingOperationEntry) FinalizeVolumeMigrate(v *VolumeEntry) {
	v.Pending.Id = ""
	return
}
