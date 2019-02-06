package model

type DeletionMarker interface {
	MarkDelete()
	IsDeleted() bool
}
