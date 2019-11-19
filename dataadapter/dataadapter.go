package dataadapter

import (
	"crypto/md5"
	"database/sql"
	"log"
	"math/big"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

const (
	CurrentUpload    = 0
	SuccessfulUpload = 1
	FailedUpload     = 2
)

type TextData struct {
	id            uint64
	time_created  int64
	title         string
	text_data     string
	upload_status int
	link          string
}

type UploadError struct {
	status int
}

func (e *UploadError) Error() string {
	if e.status == CurrentUpload {
		return "Upload in progress"
	}
	return "Failed to upload"
}

func generateId(key string, timestamp int64) uint64 {
	timestring := strconv.FormatInt(timestamp, 10)
	h := md5.New()
	h.Write([]byte(key + timestring))
	id := big.NewInt(0)
	id.SetBytes(h.Sum(nil))
	return id.Uint64()
}

type DataAdapter struct {
	db *sql.DB
}

func NewDataAdapter() *DataAdapter {
	db, err := sql.Open("mysql", "user:password@tcp(mysql:3306)/db")
	if err != nil {
		log.Fatal(err.Error())
	}
	// err = db.Ping()
	// if err != nil {
	// 	log.Fatal(err.Error())
	// }
	return &DataAdapter{db}

}

func (adapter *DataAdapter) Close() {
	adapter.db.Close()
}
func (adapter *DataAdapter) StoreTextData(title string, textBlob string) (uint64, error) {
	stmsIns, err := adapter.db.Prepare("Insert into hosted_data(id, time_created, title, upload_status) VALUES(?,?,?,?)")
	if err != nil {
		panic(err)
	}
	defer stmsIns.Close()

	curTime := time.Now().Unix()
	id := generateId(title, curTime)

	_, err = stmsIns.Exec(id, curTime, title, SuccessfulUpload)
	for err != nil {
		sqlErr, ok := err.(*mysql.MySQLError)
		if !ok {
			return 0, err
		}
		// primary key violation
		if sqlErr.Number == 2627 {
			curTime = time.Now().Unix()
			id = generateId(title, curTime)
			_, err = stmsIns.Exec(id, curTime, title, SuccessfulUpload)
		} else {
			return 0, err
		}
	}
	return id, err
}

func (adapter *DataAdapter) ReadTextData(id uint64) (string, error) {
	stmtOut, _ := adapter.db.Prepare("SELECT title,upload_status from hosted_data WHERE id = ? AND time_created + 604800 > ?")
	var resultData TextData
	err := stmtOut.QueryRow(id, time.Now().Unix()).Scan(&resultData.title, &resultData.upload_status)
	if err != nil {
		return "", err
	}
	if resultData.upload_status == SuccessfulUpload {
		return resultData.title, nil
	}
	return "", &UploadError{resultData.upload_status}

}

func (adapter *DataAdapter) GetLinkArchive(id uint64) (string, error) {
	stmtOut, _ := adapter.db.Prepare("SELECT title,upload_status,text_data from hosted_data WHERE id = ? AND time_created + 604800 > ?")
	var resultData TextData
	err := stmtOut.QueryRow(id, time.Now().Unix()).Scan(&resultData.title, &resultData.upload_status, &resultData.text_data)
	if err != nil {
		return "", err
	}
	if resultData.upload_status == SuccessfulUpload {
		return resultData.text_data, nil
	}
	return "", &UploadError{resultData.upload_status}
}

func (adapter *DataAdapter) InitLinkData(title, link string) (uint64, error) {
	stmsIns, err := adapter.db.Prepare("Insert into hosted_data(id, time_created, title, upload_status, link) VALUES(?,?,?,?,?)")
	if err != nil {
		panic(err)
	}
	defer stmsIns.Close()

	curTime := time.Now().Unix()
	id := generateId(title, curTime)

	_, err = stmsIns.Exec(id, curTime, title, CurrentUpload, link)
	for err != nil {
		sqlErr, ok := err.(*mysql.MySQLError)
		if !ok {
			return 0, err
		}
		// primary key violation
		if sqlErr.Number == 2627 {
			curTime = time.Now().Unix()
			id = generateId(title, curTime)
			_, err = stmsIns.Exec(id, curTime, title, CurrentUpload, link)
		} else {
			return 0, err
		}
	}

	//_, err = adapter.db.Query(fmt.Sprintf("Update hosted_data set upload_status = %d where id = %v", SuccessfulUpload, id))
	return id, err
}

func (adapter *DataAdapter) UpdateLinkUploaded(id uint64, link, text string, uploadStatus int) error {
	stmsIns, err := adapter.db.Prepare("Update hosted_data SET text_data = ?, upload_status = ?  Where id = ? and link = ? and upload_status = ?") // and time_created + 120 > ?")
	if err != nil {
		panic(err)
	}
	defer stmsIns.Close()

	//curTime := time.Now().Unix()

	_, err = stmsIns.Exec(text, uploadStatus, id, link, CurrentUpload)
	return err
}
