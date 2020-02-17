package userserver

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/kbuci/multiuser-weblink-store/dataadapter"
	"github.com/kbuci/multiuser-weblink-store/jobqueue"
)

type TextBody struct {
	AliveMinutes int64  `json:"alive_minutes"`
	Text         string `json:"text"`
	Id           uint64 `json:"id"`
}

type LinkBody struct {
	AliveMinutes int64  `json:"alive_minutes"`
	Link         string `json:"link"`
	Id           uint64 `json:"id"`
}

type StoreRequestBody struct {
	Error string `json:"error"`
}

type UserServer struct {
	Adapter  *dataadapter.DataAdapter
	Producer *jobqueue.JobProducer
}

const (
	BadRequestMsg       = "Ill-formed API request"
	BadUploadMsg        = "Issue downloading file from URL"
	IncompleteUploadMsg = "File not ready yet"
	NotFoundMsg         = "File/ID not valid"
	IdNotClaimed        = "ID not claimed"
)

func (server *UserServer) StoreData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	requestBody, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	var storeData TextBody

	err := json.Unmarshal(requestBody, &storeData)
	if err != nil {
		log.Printf(err.Error())
		http.Error(w, BadRequestMsg, http.StatusBadRequest)
		return
	}
	log.Print(storeData.AliveMinutes)
	claimed, err := server.Adapter.StoreTextData(storeData.AliveMinutes, storeData.Text, storeData.Id)
	if err == nil {
		w.WriteHeader(200)
		if !claimed {
			json.NewEncoder(w).Encode(StoreRequestBody{IdNotClaimed})
		} else {
			json.NewEncoder(w).Encode(StoreRequestBody{})
		}

	} else {
		log.Printf(err.Error())
		http.Error(w, "Internal error", http.StatusInsufficientStorage)
	}
}

func (server *UserServer) GetTextData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	id, err := strconv.ParseUint(mux.Vars(r)["id"], 10, 64)
	if err != nil {
		http.Error(w, BadRequestMsg, http.StatusBadRequest)
		return
	}

	data, err := server.Adapter.ReadTextData(id)
	if err == nil {
		js, _ := json.Marshal(TextBody{Text: data})
		w.WriteHeader(200)
		w.Write(js)

	} else {
		log.Printf(err.Error())
		_, ok := err.(*dataadapter.UploadError)
		if ok {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			http.Error(w, NotFoundMsg, http.StatusNotFound)
		}
	}
}

func (server *UserServer) GetArchivedLink(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(mux.Vars(r)["id"], 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	data, err := server.Adapter.GetLinkArchive(id)
	if err == nil {

		http.ServeFile(w, r, data)

	} else {
		log.Printf(err.Error())
		_, ok := err.(*dataadapter.UploadError)
		if ok {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			http.Error(w, NotFoundMsg, http.StatusNotFound)
		}
	}
}

func (server *UserServer) InitLinkData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	requestBody, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	var storeData LinkBody

	err := json.Unmarshal(requestBody, &storeData)
	if err != nil {
		log.Printf(err.Error())
		http.Error(w, BadRequestMsg, http.StatusBadRequest)
		return
	}
	log.Printf(storeData.Link)
	claimed, err := server.Adapter.InitLinkData(storeData.AliveMinutes, storeData.Link, storeData.Id)
	if err != nil {
		log.Printf(err.Error())
		http.Error(w, "Internal error", http.StatusInsufficientStorage)
		return
	} else {
		w.WriteHeader(200)
		if !claimed {
			json.NewEncoder(w).Encode(StoreRequestBody{IdNotClaimed})
		} else {
			json.NewEncoder(w).Encode(StoreRequestBody{})
		}
	}

	timeLeft, err := server.Adapter.TimeUntilDomainPoll(storeData.Link)
	if timeLeft == 0 {
		err = server.Producer.QueueLinkCopyJob(storeData.Id, storeData.Link)
	} else {
		err = server.Producer.QueueLinkDelayJob(storeData.Id, storeData.Link, timeLeft+time.Now().Unix())
	}
	if err == nil {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(StoreRequestBody{})

	} else {
		log.Printf(err.Error())
		http.Error(w, "Internal error", http.StatusInternalServerError)
	}

	log.Printf("queued %s", storeData.Link)
}

func (server *UserServer) Close() {
	server.Adapter.Close()
}

func NewRequestServer() *UserServer {
	adapter := dataadapter.NewDataAdapter()
	producer := jobqueue.NewProducer()
	return &UserServer{adapter, producer}
}
