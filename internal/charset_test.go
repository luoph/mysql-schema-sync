// Copyright(C) 2022 github.com/fsgo  All Rights Reserved.
// Author: hidu <duv123@gmail.com>
// Date: 2022/9/25

package internal

import (
	"fmt"
	"testing"

	"github.com/xanygo/anygo/xt"
)

func TestFieldInfo_CharsetCollationComparison(t *testing.T) {
	// nil charset/collation vs explicit charset/collation should be considered different
	sourceField := &FieldInfo{
		ColumnName:    "name",
		ColumnType:    "varchar(64)",
		IsNullAble:    "NO",
		CharsetName:   nil,
		CollationName: nil,
	}

	destField := &FieldInfo{
		ColumnName:    "name",
		ColumnType:    "varchar(64)",
		IsNullAble:    "NO",
		CharsetName:   stringPtr("utf8mb4"),
		CollationName: stringPtr("utf8mb4_general_ci"),
	}

	// nil vs non-nil should be different (strict comparison)
	xt.False(t, sourceField.Equals(destField))
	xt.False(t, destField.Equals(sourceField))

	// Same explicit values should be equal
	sourceField2 := &FieldInfo{
		ColumnName:    "name",
		ColumnType:    "varchar(64)",
		IsNullAble:    "NO",
		CharsetName:   stringPtr("utf8mb4"),
		CollationName: stringPtr("utf8mb4_general_ci"),
	}
	xt.True(t, sourceField2.Equals(destField))
	xt.True(t, destField.Equals(sourceField2))
}

func TestFieldInfo_DifferentCharsetCollation(t *testing.T) {
	// Test fields with actually different charset/collation
	sourceField := &FieldInfo{
		ColumnName:    "name",
		ColumnType:    "varchar(64)",
		IsNullAble:    "NO",
		CharsetName:   stringPtr("latin1"),
		CollationName: stringPtr("latin1_swedish_ci"),
	}

	destField := &FieldInfo{
		ColumnName:    "name",
		ColumnType:    "varchar(64)",
		IsNullAble:    "NO",
		CharsetName:   stringPtr("utf8mb4"),
		CollationName: stringPtr("utf8mb4_general_ci"),
	}

	// These should be considered different
	xt.False(t, sourceField.Equals(destField))
	xt.False(t, destField.Equals(sourceField))
}

func TestFieldInfo_ImplicitCharsetFromCollation(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		columnType string
		dataType   string
		collation  string
		charset    string
	}{
		{
			name:       "enum",
			columnName: "phase",
			columnType: "enum('TRANSCRIPTION','ORGANIZING')",
			dataType:   "enum",
			collation:  "utf8mb4_general_ci",
			charset:    "utf8mb4",
		},
		{
			name:       "set",
			columnName: "flags",
			columnType: "set('red','green','blue')",
			dataType:   "set",
			collation:  "utf8mb4_general_ci",
			charset:    "utf8mb4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceField := &FieldInfo{
				ColumnName:    tt.columnName,
				ColumnType:    tt.columnType,
				DataType:      tt.dataType,
				IsNullAble:    "NO",
				CharsetName:   nil,
				CollationName: stringPtr(tt.collation),
			}

			destField := &FieldInfo{
				ColumnName:    tt.columnName,
				ColumnType:    tt.columnType,
				DataType:      tt.dataType,
				IsNullAble:    "NO",
				CharsetName:   stringPtr(tt.charset),
				CollationName: stringPtr(tt.collation),
			}

			xt.True(t, sourceField.Equals(destField))
			xt.True(t, destField.Equals(sourceField))
		})
	}
}

func TestFieldInfo_WithTimestamps(t *testing.T) {
	// Timestamp fields: both sides have nil charset/collation (non-string type) → equal
	// String fields: both sides should have same charset/collation from INFORMATION_SCHEMA
	sourceFields := map[string]*FieldInfo{
		"name": {
			ColumnName:    "name",
			ColumnType:    "varchar(64)",
			IsNullAble:    "NO",
			CharsetName:   stringPtr("utf8mb4"),
			CollationName: stringPtr("utf8mb4_general_ci"),
		},
		"lock_until": {
			ColumnName:    "lock_until",
			ColumnType:    "timestamp(3)",
			IsNullAble:    "NO",
			ColumnDefault: stringPtr("CURRENT_TIMESTAMP(3)"),
			Extra:         "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(3)",
			CharsetName:   nil,
			CollationName: nil,
		},
		"locked_at": {
			ColumnName:    "locked_at",
			ColumnType:    "timestamp(3)",
			IsNullAble:    "NO",
			ColumnDefault: stringPtr("CURRENT_TIMESTAMP(3)"),
			Extra:         "DEFAULT_GENERATED",
			CharsetName:   nil,
			CollationName: nil,
		},
		"locked_by": {
			ColumnName:    "locked_by",
			ColumnType:    "varchar(255)",
			IsNullAble:    "NO",
			CharsetName:   stringPtr("utf8mb4"),
			CollationName: stringPtr("utf8mb4_general_ci"),
		},
	}

	destFields := map[string]*FieldInfo{
		"name": {
			ColumnName:    "name",
			ColumnType:    "varchar(64)",
			IsNullAble:    "NO",
			CharsetName:   stringPtr("utf8mb4"),
			CollationName: stringPtr("utf8mb4_general_ci"),
		},
		"lock_until": {
			ColumnName:    "lock_until",
			ColumnType:    "timestamp(3)",
			IsNullAble:    "NO",
			ColumnDefault: stringPtr("CURRENT_TIMESTAMP(3)"),
			Extra:         "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(3)",
			CharsetName:   nil,
			CollationName: nil,
		},
		"locked_at": {
			ColumnName:    "locked_at",
			ColumnType:    "timestamp(3)",
			IsNullAble:    "NO",
			ColumnDefault: stringPtr("CURRENT_TIMESTAMP(3)"),
			Extra:         "DEFAULT_GENERATED",
			CharsetName:   nil,
			CollationName: nil,
		},
		"locked_by": {
			ColumnName:    "locked_by",
			ColumnType:    "varchar(255)",
			IsNullAble:    "NO",
			CharsetName:   stringPtr("utf8mb4"),
			CollationName: stringPtr("utf8mb4_general_ci"),
		},
	}

	// All fields should be considered equal (same charset/collation on both sides)
	for fieldName, sourceField := range sourceFields {
		t.Run(fmt.Sprintf("field_%s", fieldName), func(t *testing.T) {
			destField := destFields[fieldName]
			xt.True(t, sourceField.Equals(destField))
			xt.True(t, destField.Equals(sourceField))
		})
	}
}

func TestFieldInfo_DefaultCharsets(t *testing.T) {
	// Strict charset/collation comparison tests
	testCases := []struct {
		name           string
		charsetName1   *string
		collationName1 *string
		charsetName2   *string
		collationName2 *string
		shouldEqual    bool
	}{
		{
			name:           "both nil - equal (non-string columns)",
			charsetName1:   nil,
			collationName1: nil,
			charsetName2:   nil,
			collationName2: nil,
			shouldEqual:    true,
		},
		{
			name:           "nil vs utf8mb4 - different",
			charsetName1:   nil,
			collationName1: nil,
			charsetName2:   stringPtr("utf8mb4"),
			collationName2: stringPtr("utf8mb4_general_ci"),
			shouldEqual:    false,
		},
		{
			name:           "nil charset vs explicit charset only - different",
			charsetName1:   nil,
			collationName1: nil,
			charsetName2:   stringPtr("utf8mb4"),
			collationName2: nil,
			shouldEqual:    false,
		},
		{
			name:           "nil collation vs explicit collation only - different",
			charsetName1:   nil,
			collationName1: nil,
			charsetName2:   nil,
			collationName2: stringPtr("utf8mb4_general_ci"),
			shouldEqual:    false,
		},
		{
			name:           "same utf8mb4 general_ci - equal",
			charsetName1:   stringPtr("utf8mb4"),
			collationName1: stringPtr("utf8mb4_general_ci"),
			charsetName2:   stringPtr("utf8mb4"),
			collationName2: stringPtr("utf8mb4_general_ci"),
			shouldEqual:    true,
		},
		{
			name:           "same utf8 general_ci - equal",
			charsetName1:   stringPtr("utf8"),
			collationName1: stringPtr("utf8_general_ci"),
			charsetName2:   stringPtr("utf8"),
			collationName2: stringPtr("utf8_general_ci"),
			shouldEqual:    true,
		},
		{
			name:           "same latin1 swedish_ci - equal",
			charsetName1:   stringPtr("latin1"),
			collationName1: stringPtr("latin1_swedish_ci"),
			charsetName2:   stringPtr("latin1"),
			collationName2: stringPtr("latin1_swedish_ci"),
			shouldEqual:    true,
		},
		{
			name:           "different charset: ascii vs utf8mb4 - different",
			charsetName1:   stringPtr("ascii"),
			collationName1: stringPtr("ascii_general_ci"),
			charsetName2:   stringPtr("utf8mb4"),
			collationName2: stringPtr("utf8mb4_general_ci"),
			shouldEqual:    false,
		},
		{
			name:           "same charset different collation - different",
			charsetName1:   stringPtr("utf8mb4"),
			collationName1: stringPtr("utf8mb4_general_ci"),
			charsetName2:   stringPtr("utf8mb4"),
			collationName2: stringPtr("utf8mb4_unicode_ci"),
			shouldEqual:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			field1 := &FieldInfo{
				ColumnName:    "test_field",
				ColumnType:    "varchar(100)",
				IsNullAble:    "NO",
				CharsetName:   tc.charsetName1,
				CollationName: tc.collationName1,
			}

			field2 := &FieldInfo{
				ColumnName:    "test_field",
				ColumnType:    "varchar(100)",
				IsNullAble:    "NO",
				CharsetName:   tc.charsetName2,
				CollationName: tc.collationName2,
			}

			if tc.shouldEqual {
				xt.True(t, field1.Equals(field2))
				xt.True(t, field2.Equals(field1))
			} else {
				xt.False(t, field1.Equals(field2))
				xt.False(t, field2.Equals(field1))
			}
		})
	}
}
