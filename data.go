package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"strconv"
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

type Category struct {
	ID               int
	DisplayName      string
	DefinitionString string
	Definition       CategoryDefinition
}

type CategoryDefinition struct {
	DatabaseName string           `json:"databaseName"`
	Columns      []CategoryColumn `json:"columns"`
}

type CategoryColumn struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
	Type        string `json:"type"`
	Default     string `json:"default"`
	Incrementer bool   `json:"incrementer"`
	IntWithDot  bool   `json:"intWithDot"`
}

func (c *Category) TransformToJsonString() error {
	return json.Unmarshal([]byte(c.DefinitionString), &c.Definition)
}

func (c *Category) TransformFromJsonString() error {
	t, err := json.Marshal(&c.Definition)
	c.DefinitionString = string(t)
	return err
}

func getCategoryNames() ([]string, error) {

	rows, err := db.Query("SELECT NAME from master;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string

	for rows.Next() {
		var displayName string
		if err = rows.Scan(&displayName); err != nil {
			return nil, err
		}
		names = append(names, displayName)
	}
	return names, nil
}

func getCategories(name string) ([]Category, error) {
	rows, err := db.Query("SELECT ID, NAME, DEFINITION from master WHERE NAME=? AND ? != '';",
		name, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var categories []Category

	for rows.Next() {
		var category Category
		if err = rows.Scan(&category.ID, &category.DisplayName, &category.DefinitionString); err != nil {
			return nil, err
		}
		if err = category.TransformFromJsonString(); err != nil {
			return nil, err
		}
		categories = append(categories, category)
	}
	return categories, nil
}

type Overview struct {
	Name              string
	Columns           []string
	ColHasIncrementer []bool
	ColDisplaySpecial []bool
	Values            [][]string
}

func (o *Overview) HasIncrementer(pos int) bool {
	return o.ColHasIncrementer[pos]
}

func (o *Overview) Display(pos int, text string) string {
	if o.ColDisplaySpecial[pos] && len(text) != 0 {
		if len(text) == 1 {
			return "0." + text
		}
		return text[:len(text)-1] + "." + text[len(text)-1:]
	}
	return text
}

func getOverview(name string) ([]Overview, error) {
	categories, err := getCategories(name)
	if err != nil {
		return nil, err
	}

	var overviews []Overview

	for _, category := range categories {
		var overview Overview
		overview.Name = category.DisplayName

		overview.Columns = make([]string, len(category.Definition.Columns))
		overview.ColHasIncrementer = make([]bool, len(category.Definition.Columns))
		overview.ColDisplaySpecial = make([]bool, len(category.Definition.Columns))
		for i, value := range category.Definition.Columns {
			overview.Columns[i] = value.DisplayName
			overview.ColHasIncrementer[i] = value.Incrementer
			overview.ColDisplaySpecial[i] = value.IntWithDot
		}

		scanTargets, values := buildScanTargets(category.Definition.Columns)
		var rows *sql.Rows
		rows, err = db.Query("SELECT * FROM " + category.Definition.DatabaseName + ";")
		if err != nil {
			return nil, err
		}

		overview.Values = make([][]string, 0)

		for rows.Next() {
			err = rows.Scan(scanTargets...)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}

			overview.Values = append(overview.Values, extractStrings(values))
		}
		_ = rows.Close()
	}
	return overviews, nil
}

func buildScanTargets(cols []CategoryColumn) ([]any, []any) {
	scanTargets := make([]any, len(cols))
	values := make([]any, len(cols))

	for i, c := range cols {
		ptr := goTypeFromColumnType(c.Type)
		scanTargets[i] = ptr
		values[i] = ptr
	}

	return scanTargets, values
}

func goTypeFromColumnType(t string) any {
	switch t {
	case "TEXT":
		var v sql.NullString
		return &v
	case "INTEGER":
		var v sql.NullInt64
		return &v
	case "BOOLEAN":
		var v sql.NullBool
		return &v
	default:
		var v sql.NullString
		return &v
	}
}

func extractTyped(values []any, cols []CategoryColumn) []any {
	result := make([]any, len(cols))

	for i, v := range values {
		switch val := v.(type) {
		case *sql.NullString:
			if val.Valid {
				result[i] = val.String
			}
		case *sql.NullInt64:
			if val.Valid {
				result[i] = int(val.Int64)
			}
		case *sql.NullBool:
			if val.Valid {
				result[i] = val.Bool
			}
		default:
			result[i] = nil
		}
	}

	return result
}

func extractStrings(values []any) []string {
	result := make([]string, len(values))

	for i, v := range values {
		switch val := v.(type) {
		case *sql.NullString:
			if val.Valid {
				result[i] = val.String
			}
		case *sql.NullInt64:
			if val.Valid {
				result[i] = strconv.FormatInt(val.Int64, 10)
			}
		case *sql.NullBool:
			if val.Valid {
				if val.Bool {
					result[i] = "✓"
				} else {
					result[i] = "✗"
				}
			}
		default:
			result[i] = ""
		}
	}

	return result
}
