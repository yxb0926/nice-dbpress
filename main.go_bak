package main

import "fmt"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"

var c chan int

func db_sel() {
	//db, _ := sql.Open("mysql", "yxb:yxb@tcp(127.0.0.1:3306)/test?timeout=4s&charset=utf8")
	//sql := "SELECT 1 AS id"

	var i int = 0
	for i < 100000 {
		db, _ := sql.Open("mysql", "yxb:yxb@tcp(127.0.0.1:3306)/test?timeout=4s&charset=utf8")
		sql := "SELECT 1 AS id"
		rows := db.QueryRow(sql)
		var id int
		err := rows.Scan(&id)
		if err != nil {
			fmt.Println(err)
		}
		db.Close()
		i++
	}
	c <- 1
}

func main() {
	c = make(chan int)
	var i int = 0
	for i < 100 {
		go db_sel()
		i++
	}
	var j int = 0
	for j < 100 {
		<-c
		j++
	}
}
