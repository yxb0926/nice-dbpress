package main

import "fmt"
import "os"
import "github.com/larspensjo/config"
import "flag"
import "strconv"
import "time"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import "runtime"

var confile string

type dsninfo struct {
	hostname string
	username string
	password string
	port     string
	timeout  string
	charset  string
}

var dsn dsninfo
var keepalived int = 1
var currentnums int = 1
var logfile string
var sqlstr string
var presstime int64 = 300
var c chan int
var dbtype string = "mysql"
var connstr string = ""

const (
	CURRENTNUMS = 8
	KEEPALIVED  = 1
	LOGFILE     = "/tmp/dbpress.log"
	PRESSTIME   = 300
)

func init() {
	// get the conf file
	GetIniFile()
	// Parse the conf file
	ParseConfile("dbpress")

	setConnstr()
}

func main() {
	fmt.Println("## main ")
	runtime.GOMAXPROCS(32)

	c = make(chan int)
	i := 0
	for i < currentnums {
		if keepalived == 1 {
			go LongconnPress()
			i++
		} else if keepalived == 0 {
			go ShortconnPress()
			i++
		}
	}
	j := 0
	for j < currentnums {
		<-c
		j++
	}
}

// 和mysqld 保持长连接
func LongconnPress() {
	j := time.Now().Unix() + presstime
	db, _ := sql.Open(dbtype, connstr)
	for {
		runtime.Gosched()
		if j < time.Now().Unix() {
			break
		}
		sql := "SELECT 1 AS id"
		rows := db.QueryRow(sql)
		var id int
		err := rows.Scan(&id)
		if err != nil {
			fmt.Println(err)
		}
	}
	db.Close()
	c <- 1
}

// 和mysqld 保持短连接
func ShortconnPress() {
	j := time.Now().Unix() + presstime
	for {
		runtime.Gosched()
		if j < time.Now().Unix() {
			break
		}
		db, _ := sql.Open(dbtype, connstr)
		sql := "SELECT 1 AS id"
		rows := db.QueryRow(sql)
		var id int
		err := rows.Scan(&id)
		if err != nil {
			fmt.Println(err)
		}
		db.Close()
	}
	c <- 1
}

func ParseConfile(key string) {
	conf := make(map[string]string)
	cfg, err := config.ReadDefault(confile)
	if err != nil {
		fmt.Errorf("Failed Read cfile")
	}

	if cfg.HasSection(key) {
		section, err := cfg.SectionOptions(key)
		if err == nil {
			for _, v := range section {
				options, err := cfg.String(key, v)
				if err == nil {
					conf[v] = options
				}
			}
		}
		if v, ok := conf["host"]; ok {
			dsn.hostname = v
		} else {
			fmt.Errorf("Key:host Not Found In The Config file.")
		}
		if v, ok := conf["username"]; ok {
			dsn.username = v
		} else {
			fmt.Errorf("Key:username Not Found In The Config file.")
		}
		if v, ok := conf["password"]; ok {
			dsn.password = v
		} else {
			fmt.Errorf("Key:password Not Found In The Config file.")
		}
		if v, ok := conf["port"]; ok {
			dsn.port = v
		} else {
			fmt.Errorf("Key:port Not Found In The Config file.")
		}
		if v, ok := conf["timeout"]; ok {
			dsn.timeout = v
		} else {
			fmt.Errorf("Key:timeout Not Found In The Config file.")
		}
		if v, ok := conf["charset"]; ok {
			dsn.charset = v
		} else {
			fmt.Errorf("Key:charset Not Found In The Config file.")
		}
		if v, ok := conf["currentnums"]; ok {
			currentnums, _ = strconv.Atoi(v)
		} else {
			currentnums = CURRENTNUMS
		}
		if v, ok := conf["keepalived"]; ok {
			keepalived, _ = strconv.Atoi(v)
		} else {
			currentnums = KEEPALIVED
		}
		if v, ok := conf["logfile"]; ok {
			logfile = v
		} else {
			logfile = LOGFILE
		}
		if v, ok := conf["presstime"]; ok {
			presstime, _ = strconv.ParseInt(v, 10, 64)
		} else {
			presstime = PRESSTIME
		}
		if v, ok := conf["sqljson"]; ok {
			sqlstr = v
		} else {
			fmt.Errorf("Key:sqljson Not Found In The Config file.")
		}
	} else {
		fmt.Errorf("The conf file have no section %s", key)
	}
}

func GetIniFile() string {
	iniFile := flag.String("f", "iniFile", "The confile file name")
	flag.Parse()

	if flag.NFlag() == 0 {
		help()
		os.Exit(0)
	}

	if _, err := os.Stat(*iniFile); err == nil {
		confile = *iniFile
	} else {
		fmt.Println("The confile", *iniFile, "not exist.")
		os.Exit(7)
	}

	return confile
}

func setConnstr() {
	connstr += dsn.username
	connstr += ":"
	connstr += dsn.password
	connstr += "@tcp("
	connstr += dsn.hostname
	connstr += ":"
	connstr += dsn.port
	connstr += ")/?timeout="
	connstr += dsn.timeout
	connstr += "&charset="
	connstr += dsn.charset
}

func help() {
	var helpx string = `dbpress usage:
	-f="iniFile": The confile file name
	`

	fmt.Println(helpx)
}
