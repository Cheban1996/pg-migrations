package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"io"
	"io/ioutil"
	"log"
	"os"
)

//  The maximum length of a query that PostgreSQL can process is 2147483648 characters

const LengthQueryPG = 2147483648

func InitDB() (*sql.DB, func(), error) {
	connStr := "user=postgres password=postgres dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	return db, func() { db.Close() }, nil
}

func main() {
	fmt.Println("***GO APPLY MIGRATIONS***")

	// INIT CONNECT TO DB

	db, closeDB, err := InitDB()
	if err != nil {
		panic("ERROR DB")
	}

	defer closeDB()

	initTableMigration(db)
	nameMigrations := getMigrations(db)

	var migrations = map[string]string{}

	files, err := ioutil.ReadDir("./versions")
	if err != nil {
		log.Fatal(err)
	}
	for _, fileName := range files {
		path := fmt.Sprintf("./versions/%s", fileName.Name())
		file, _ := os.Open(path)
		defer file.Close()

		data := make([]byte, LengthQueryPG)
		n, err := file.Read(data)
		if err == io.EOF {
			break
		}
		migrations[fileName.Name()] = string(data[:n])
	}

	for name, text := range migrations {
		if In(name, nameMigrations) {
			continue
		}
		_, err := db.Exec(text)
		if err != nil {
			panic(err)
		}

		applyMigration(db, name)
		fmt.Println("Applied ", name)
	}

}

func addMigration() {

}

func initTableMigration(db *sql.DB) {
	var query = `CREATE TABLE IF NOT EXISTS migrations (
				 name VARCHAR(128) PRIMARY KEY 
				)`
	_, err := db.Exec(query)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func applyMigration(db *sql.DB, name string) {
	var query = `INSERT INTO migrations (name) VALUES ($1)`
	_, err := db.Exec(query, name)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func getMigrations(db *sql.DB) []string {
	var query = "SELECT * FROM migrations"
	rows, err := db.Query(query)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer rows.Close()

	var migrations []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			fmt.Println(err)
			continue
		}
		migrations = append(migrations, name)
	}
	return migrations
}

func In(v string, values []string) bool {
	for _, val := range values {
		if v == val {
			return true
		}
	}
	return false
}
