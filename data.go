package main

import (
	"database/sql"
	"log"
)

const MasterTable = "master"

var db *sql.DB = nil

func logOntoDB() {
	var err error
	db, err = sql.Open("sqlite", "./data.sqlite")
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Connected to the SQLite database successfully.")
	checkBaseTable()
}

func checkBaseTable() {
	var err error
	var amountMasterTables int
	err = db.QueryRow("SELECT COUNT(*) AS nums FROM sqlite_master WHERE type='table' AND name=?;", MasterTable).Scan(&amountMasterTables)
	if err != nil {
		log.Fatalln(err)
	}
	if amountMasterTables < 1 {
		_, err = db.Exec(`
CREATE TABLE master (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    NAME TEXT,
    DEFINITION TEXT
)       `)
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func closeDB() {
	db.Close()
}

func getCategoryNames() ([]string, error) {
	rows, err := db.Query("SELECT NAME from master;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string

	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

type Overview struct {
	Name    string
	Columns []string
	Values  [][]string
}

func getOverview(name string) ([]Overview, error) {
	return nil, nil
}
