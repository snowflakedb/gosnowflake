package gosnowflake

import (
	"database/sql"
	"testing"
)

func TestBindingVariant(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		skipStructuredTypesTestsOnGHActions(t)
		dbt.mustExec("CREATE TABLE test_variant_binding (var VARIANT)")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_variant_binding")
		}()
		dbt.mustExec("ALTER SESSION SET ENABLE_OBJECT_TYPED_BINDS = true")
		dbt.mustExec("ALTER SESSION SET ENABLE_STRUCTURED_TYPES_IN_BINDS = Enable")
		dbt.mustExec("INSERT INTO test_variant_binding SELECT (?)", DataTypeVariant, nil)
		dbt.mustExec("INSERT INTO test_variant_binding SELECT (?)", DataTypeVariant, sql.NullString{Valid: false})
		dbt.mustExec("INSERT INTO test_variant_binding SELECT (?)", DataTypeVariant, "{'s': 'some string'}")
		dbt.mustExec("INSERT INTO test_variant_binding SELECT (?)", DataTypeVariant, sql.NullString{Valid: true, String: "{'s': 'some string2'}"})
		rows := dbt.mustQuery("SELECT * FROM test_variant_binding")
		defer rows.Close()
		var res sql.NullString

		assertTrueF(t, rows.Next())
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertFalseF(t, res.Valid)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertFalseF(t, res.Valid)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertTrueE(t, res.Valid)
		assertEqualIgnoringWhitespaceE(t, res.String, `{"s": "some string"}`)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertTrueE(t, res.Valid)
		assertEqualIgnoringWhitespaceE(t, res.String, `{"s": "some string2"}`)
	})
}

func TestBindingObjectWithoutSchema(t *testing.T) {
	skipStructuredTypesTestsOnGHActions(t)
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test_object_binding (obj OBJECT)")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_object_binding")
		}()
		dbt.mustExec("ALTER SESSION SET ENABLE_OBJECT_TYPED_BINDS = true")
		dbt.mustExec("ALTER SESSION SET ENABLE_STRUCTURED_TYPES_IN_BINDS = Enable")
		dbt.mustExec("INSERT INTO test_object_binding SELECT (?)", DataTypeObject, nil)
		dbt.mustExec("INSERT INTO test_object_binding SELECT (?)", DataTypeObject, sql.NullString{Valid: false})
		dbt.mustExec("INSERT INTO test_object_binding SELECT (?)", DataTypeObject, "{'s': 'some string'}")
		dbt.mustExec("INSERT INTO test_object_binding SELECT (?)", DataTypeObject, sql.NullString{Valid: true, String: "{'s': 'some string2'}"})
		rows := dbt.mustQuery("SELECT * FROM test_object_binding")
		defer rows.Close()
		var res sql.NullString

		assertTrueF(t, rows.Next())
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertFalseF(t, res.Valid)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertFalseF(t, res.Valid)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertTrueE(t, res.Valid)
		assertEqualIgnoringWhitespaceE(t, res.String, `{"s": "some string"}`)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertTrueE(t, res.Valid)
		assertEqualIgnoringWhitespaceE(t, res.String, `{"s": "some string2"}`)
	})
}

func TestBindingArrayWithoutSchema(t *testing.T) {
	skipStructuredTypesTestsOnGHActions(t)
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test_array_binding (arr ARRAY)")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_array_binding")
		}()
		dbt.mustExec("ALTER SESSION SET ENABLE_OBJECT_TYPED_BINDS = true")
		dbt.mustExec("ALTER SESSION SET ENABLE_STRUCTURED_TYPES_IN_BINDS = Enable")
		dbt.mustExec("INSERT INTO test_array_binding SELECT (?)", DataTypeArray, nil)
		dbt.mustExec("INSERT INTO test_array_binding SELECT (?)", DataTypeArray, sql.NullString{Valid: false})
		dbt.mustExec("INSERT INTO test_array_binding SELECT (?)", DataTypeArray, "[1, 2, 3]")
		dbt.mustExec("INSERT INTO test_array_binding SELECT (?)", DataTypeArray, sql.NullString{Valid: true, String: "[1, 2, 3]"})
		rows := dbt.mustQuery("SELECT * FROM test_array_binding")
		defer rows.Close()
		var res sql.NullString

		assertTrueF(t, rows.Next())
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertFalseF(t, res.Valid)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertFalseF(t, res.Valid)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertTrueE(t, res.Valid)
		assertEqualIgnoringWhitespaceE(t, res.String, `[1, 2, 3]`)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertTrueE(t, res.Valid)
		assertEqualIgnoringWhitespaceE(t, res.String, `[1, 2, 3]`)
	})
}
