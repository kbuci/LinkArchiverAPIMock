package dataadapter

import (
	"crypto/md5"
	"database/sql"
	"log"
	"math/big"
	"net"
	"net/url"
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
	time_expired  int64
	text_data     string
	upload_status int
	link_archive  string
	link          string
}

type PollData struct {
	last_polled int64
	poll_delay  int64
	domain      string
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

// func (adapted *DataAdapter) InitedId() (int, err){
// 	defer stmsIns.Close()

// 	curTime := time.Now().Unix()
// 	id := generateId(link, curTime)

// 	_, err = stmsIns.Exec(id, curTime+60*aliveMinutes, CurrentUpload, link)
// 	for err != nil {
// 		sqlErr, ok := err.(*mysql.MySQLError)
// 		if !ok {
// 			return 0, err
// 		}
// 		// primary key violation, happens if uuid is already assigned
// 		if sqlErr.Number == 2627 {
// 			curTime = time.Now().Unix()
// 			id = generateId(link, curTime)
// 			_, err = stmsIns.Exec(id, curTime+60*aliveMinutes, CurrentUpload, link)
// 		} else {
// 			return 0, err
// 		}
// 	}

// }
func (adapter *DataAdapter) StoreTextData(aliveMinutes int64, textBlob string, claimedId uint64) (bool, error) {
	stmsIns, err := adapter.db.Prepare("Insert into hosted_data(id, time_expired, text_data, upload_status) VALUES(?,?,?,?)")
	if err != nil {
		panic(err)
	}
	defer stmsIns.Close()

	curTime := time.Now().Unix()

	_, err = stmsIns.Exec(claimedId, curTime+60*aliveMinutes, textBlob, SuccessfulUpload)
	if err != nil {
		sqlErr, ok := err.(*mysql.MySQLError)
		if !ok {
			return true, err
		}
		// primary key violation
		if sqlErr.Number == 2627 {
			return false, nil
		} else {
			return true, err
		}
	}
	return true, nil
}

func (adapter *DataAdapter) ReadTextData(id uint64) (string, error) {
	stmtOut, _ := adapter.db.Prepare("SELECT text_data,upload_status from hosted_data WHERE id = ? AND time_expired > ?")
	var resultData TextData
	err := stmtOut.QueryRow(id, time.Now().Unix()).Scan(&resultData.text_data, &resultData.upload_status)
	if err != nil {
		return "", err
	}
	if resultData.upload_status == SuccessfulUpload {
		return resultData.text_data, nil
	}
	return "", &UploadError{resultData.upload_status}

}

func (adapter *DataAdapter) GetLinkArchive(id uint64) (string, error) {
	stmtOut, _ := adapter.db.Prepare("SELECT upload_status,link_archive from hosted_data WHERE id = ? AND time_expired > ?")
	var resultData TextData
	err := stmtOut.QueryRow(id, time.Now().Unix()).Scan(&resultData.upload_status, &resultData.link_archive)
	if err != nil {
		return "", err
	}
	if resultData.upload_status == SuccessfulUpload {
		return resultData.link_archive, nil
	}
	return "", &UploadError{resultData.upload_status}
}

func (adapter *DataAdapter) InitLinkData(aliveMinutes int64, link string, claimedId uint64) (bool, error) {
	stmsIns, err := adapter.db.Prepare("Insert into hosted_data(id, time_expired, upload_status, link) VALUES(?,?,?,?)")
	if err != nil {
		panic(err)
	}
	defer stmsIns.Close()

	curTime := time.Now().Unix()
	_, err = stmsIns.Exec(claimedId, curTime+60*aliveMinutes, CurrentUpload, link)
	if err != nil {
		sqlErr, ok := err.(*mysql.MySQLError)
		if !ok {
			return true, err
		}
		// primary key violation
		if sqlErr.Number == 2627 {
			return false, nil
		} else {
			return true, err
		}
	}
	return true, nil
}

func (adapter *DataAdapter) UpdateLinkUploaded(id uint64, link, text string, uploadStatus int) error {
	stmsIns, err := adapter.db.Prepare("Update hosted_data SET link_archive = ?, upload_status = ?  Where id = ? and link = ? and upload_status = ? and time_expired > ?")
	if err != nil {
		panic(err)
	}
	defer stmsIns.Close()

	curTime := time.Now().Unix()

	_, err = stmsIns.Exec(text, uploadStatus, id, link, CurrentUpload, curTime)
	return err
}

func (adapter *DataAdapter) TimeUntilDomainPoll(rawURL string) (int64, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return 0, err
	}
	domain, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		return 0, err
	}
	stmtOut, _ := adapter.db.Prepare("Select last_polled, poll_delay from domain_polling where domain = ?")
	defer stmtOut.Close()
	var resultData PollData
	err = stmtOut.QueryRow(domain).Scan(&resultData.last_polled, &resultData.poll_delay)
	if err != nil {
		sqlErr, ok := err.(*mysql.MySQLError)
		if !ok {
			return 0, err
		}
		if sqlErr == sql.ErrNoRows {
			resultData.last_polled = 0
			crawlDelay, err := requestCrawlPolicy(u.Path, domain)
			resultData.poll_delay = crawlDelay
			if err != nil {
				return 0, err
			}

			insDefaultStmt, _ := adapter.db.Prepare("Insert ignore hosted_data(domain, last_polled, poll_delay) VALUES(?,?,?)")
			_, err = insDefaultStmt.Exec(domain, 0, &resultData.poll_delay)
			if err != nil {
				return 0, err
			}
		} else {
			return 0, err
		}
	}
	curTime := time.Now().Unix()
	if resultData.last_polled+resultData.poll_delay > curTime {
		return resultData.last_polled + resultData.poll_delay, nil
	}

	domainUpdateStmt, err := adapter.db.Prepare("Update domain_polled SET poll_delay = ? and last_polled = ? where domain = ? and last_polled = ? ")
	if err != nil {
		panic(err)
	}
	defer domainUpdateStmt.Close()

	updateResults, err := domainUpdateStmt.Exec(resultData.poll_delay, curTime, domain, resultData.last_polled)
	if err != nil {
		return 0, err
	}
	numAffected, err := updateResults.RowsAffected()
	if err != nil {
		return 0, err
	}

	if numAffected == 0 {
		return resultData.poll_delay, nil
	}
	return 0, nil
}
