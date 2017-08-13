package metabase

import (
	"time"

	"github.com/ghetzel/pivot/dal"
)

var MetadataSchema = &dal.Collection{
	Name:              `metadata`,
	IdentityFieldType: dal.StringType,
	Fields: []dal.Field{
		{
			Name:     `name`,
			Type:     dal.StringType,
			Required: true,
		}, {
			Name:     `parent`,
			Type:     dal.StringType,
			Required: true,
		}, {
			Name: `type`,
			Type: dal.StringType,
		}, {
			Name:      `size`,
			Type:      dal.IntType,
			Validator: dal.ValidatePositiveOrZeroInteger,
		}, {
			Name: `checksum`,
			Type: dal.StringType,
		}, {
			Name:     `root_group`,
			Type:     dal.StringType,
			Required: true,
		}, {
			Name:     `group`,
			Type:     dal.BooleanType,
			Required: true,
		}, {
			Name:      `children`,
			Type:      dal.IntType,
			Validator: dal.ValidatePositiveOrZeroInteger,
		}, {
			Name:     `last_modified_at`,
			Type:     dal.IntType,
			Required: true,
		}, {
			Name:         `created_at`,
			Type:         dal.IntType,
			Required:     true,
			DefaultValue: time.Now,
		}, {
			Name: `metadata`,
			Type: dal.ObjectType,
		},
	},
}
