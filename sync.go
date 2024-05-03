package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

	_ "github.com/lib/pq"
)

func CreateYml(source_database string, target_database string) {
	content := []byte(`from: ` + source_database + `
to: ` + target_database)

	err := os.WriteFile(".pgsync.yml", content, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

func DeleteExtraColumns(table string, extraColumns []string, connStr string) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for _, column := range extraColumns {
		column = strings.TrimSpace(column)
		query := fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s;", table, column)
		_, err := db.Exec(query)
		if err != nil {
			log.Println(err)
		}
	}
}

func RunPsync(table string, connStr string) {
	cmd := exec.Command("pgsync", table)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Println(string(err.Error()))
	}
	outputStr := string(output)
	lines := strings.Split(outputStr, "\n")
	for _, line := range lines {
		if strings.Contains(line, "Extra columns") {
			parts := strings.Split(line, "Extra columns: ")
			if len(parts) > 1 {
				extraColumns := strings.Split(parts[1], ",")
				DeleteExtraColumns(table, extraColumns, connStr)
			}
		}
	}
}

func FindTarget(schema_table string) (string, bool) {
	db, err := sql.Open("postgres", admin_db)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	schema := strings.Split(schema_table, ".")[0]
	table := strings.Split(schema_table, ".")[1]

	var targetDatabase string
	var fullSync bool
	err = db.QueryRow("SELECT target_database, full_sync FROM db_admin.schema_sync where schema = $1", schema).Scan(&targetDatabase, &fullSync)
	if err != nil {
		log.Fatal(err)
	}
	if fullSync {
		return targetDatabase, fullSync
	} else {
		err = db.QueryRow("SELECT target_database FROM db_admin.table_sync where schema = $1 and table = $2", schema, table).Scan(&targetDatabase)
		if err == sql.ErrNoRows {
			return "", fullSync
		} else if err != nil {
			log.Fatal(err)
		}
		return targetDatabase, fullSync
	}
}

func GetUrls(database string) string {
	var connStr string
	if database == "datawarehouse" {
		connStr = datawarehouse
	} else if database == "datamart" {
		connStr = datamart
	}
	return connStr
}

func GetTableColumns(connStr string, schemaName string, tableName string) map[string]string {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = $1 AND table_name = $2
    `, schemaName, tableName)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	columns := make(map[string]string)
	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			log.Fatal(err)
		}
		columns[columnName] = dataType
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	db.Close()
	return columns
}

func CheckColumns(db *sql.DB, columns map[string]string, schema string, table string) {
	for columnName, dataType := range columns {
		var count int
		err := db.QueryRow(`
			SELECT count(*)
			FROM information_schema.columns 
			WHERE table_schema = $1 AND table_name = $2 AND column_name = $3 AND data_type = $4
		`, schema, table, columnName, dataType).Scan(&count)
		if err != nil {
			log.Fatal(err)
		}
		if count == 0 {
			query_drop := fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN IF EXISTS %s;", schema, table, columnName)
			_, err = db.Exec(query_drop)
			query := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s %s;", schema, table, columnName, dataType)
			_, err := db.Exec(query)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func CheckTarget(source_connStr string, target_connStr string, schema_table string) {
	schemaName := strings.Split(schema_table, ".")[0]
	tableName := strings.Split(schema_table, ".")[1]

	columns := GetTableColumns(source_connStr, schemaName, tableName)

	db, err := sql.Open("postgres", target_connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName))
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s", schemaName, tableName))
	CheckColumns(db, columns, schemaName, tableName)
}

func DeleteFile() {
	err := os.Remove(".pgsync.yml")
	if err != nil {
		log.Fatal(err)
	}
}

func SyncTable(schema_table string) {
	target_database, fullSync := FindTarget(schema_table)
	if fullSync {
		source_database := GetUrls("datawarehouse")
		target_database := GetUrls(target_database)
		CheckTarget(source_database, target_database, schema_table)
		CreateYml(source_database, target_database)
		RunPsync(schema_table, target_database)
		DeleteFile()
	}
}
