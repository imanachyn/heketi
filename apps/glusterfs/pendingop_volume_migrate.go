package glusterfs

const (
	OperationMigrateVolume PendingOperationType = iota + 10
)

const (
	OpMigrateVolume PendingChangeType = iota + 20
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
