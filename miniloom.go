package main

import (
	"database/sql"
	"log"
	"time"

	"github.com/go-redis/redis"
)

const (
	admin_db      = "postgres://localhost:5432/admin?sslmode=disable"
	datawarehouse = "postgres://localhost:5432/test_1?sslmode=disable"
	datamart      = "postgres://localhost:5432/test_2?sslmode=disable"
)

func GetSchemaTablesMap(connStr string) map[string][]string {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT table_schema, table_name 
		FROM information_schema.tables 
		WHERE table_type='BASE TABLE' 
		AND table_schema NOT IN ('pg_catalog', 'information_schema', 'public')
    `)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	schemaTables := make(map[string][]string)
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			log.Fatal(err)
		}
		schemaTables[schema] = append(schemaTables[schema], table)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	return schemaTables
}

func CheckSchema(datamart string, schema string) (bool, bool) {
	db, err := sql.Open("postgres", admin_db)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	var target string
	var full_sync bool
	err = db.QueryRow("SELECT schema, full_sync FROM db_admin.schema_sync where target_database = $1 and schema = $2", datamart, schema).Scan(&target, &full_sync)
	if err != nil {
		log.Fatal(err)
	}
	if schema == "" {
		return false, false
	} else if full_sync {
		return true, false
	} else {
		return true, true
	}
}

func DeleteSchema(connStr string, schema string) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec("DROP SCHEMA " + schema + " CASCADE")
	if err != nil {
		log.Fatal(err)
	}
}

func CheckDelete() {
	datamart_list := []string{"datamart"}
	for _, datamart := range datamart_list {
		connStr := GetUrls(datamart)
		schemaTables := GetSchemaTablesMap(connStr)
		for schema, tables := range schemaTables {
			keep_schema, check_tables := CheckSchema(datamart, schema)
			if keep_schema {
				if check_tables {
					for _, table := range tables {
						log.Println(schema, table)
						// Check if table exists in datamart
						// Check if table exists in datawarehouse
						// If table exists in datamart but not in datawarehouse, delete table in datamart
					}
				}
			} else {
				DeleteSchema(connStr, schema)
			}
		}
	}

}

func SyncWait() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	for {
		schema_table, err := client.LPop("miniloom:sync_queue").Result()
		if err != nil {
			log.Fatal(err)
		}
		if schema_table != "" {
			SyncTable(schema_table)
		} else {
			time.Sleep(30 * time.Second)
		}
	}
}

func main() {
	CheckDelete()
}
