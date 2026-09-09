package data

import (
	"database/sql"
	"errors"
)

var (
	ErrRecordNotFound = errors.New("record not found")
	ErrEditConflict   = errors.New("edit conflict")
)

type Models struct {
	Images   ImageModel
	Jobs     JobModel
	Variants VariantModel
}

func NewModels(db *sql.DB) Models {
	return Models{
		Images:   ImageModel{DB: db},
		Jobs:     JobModel{DB: db},
		Variants: VariantModel{DB: db},
	}
}
