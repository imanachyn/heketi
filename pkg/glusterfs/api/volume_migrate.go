package api

import "github.com/go-ozzo/ozzo-validation"

type VolumeMigrateRequest struct {
	NodeId string `json:"node"`
}

func (vmr VolumeMigrateRequest) Validate() error {
	return validation.ValidateStruct(&vmr,
		validation.Field(&vmr.NodeId, validation.Required, validation.By(ValidateUUID)),
	)
}
