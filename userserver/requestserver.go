package userserver

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/kbuci/text-hosting-mock/dataadapter"
	"github.com/kbuci/text-hosting-mock/jobqueue"
)

type TextBody struct {
	Title string `json:"title"`
	Text  string `json:"text"`
}

type LinkBody struct {
	Title string `json:"title"`
	Link  string `json:"link"`
}

type IdBody struct {
	Id uint64 `json:"id"`
}

type UserServer struct {
	Adapter  *dataadapter.DataAdapter
	Producer *jobqueue.JobProducer
}

const (
	BadRequestMsg       = "Ill-formed API request"
	BadUploadMsg        = "Issue archiving file"
	IncompleteUploadMsg = "File not ready yet"
	NotFoundMsg         = "File/ID not valid"
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
	log.Printf(storeData.Title)
	claimedId, err := server.Adapter.StoreTextData(storeData.Title, storeData.Text)
	if err == nil {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(IdBody{claimedId})

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
		js, _ := json.Marshal(TextBody{Title: data})
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
	claimedId, err := server.Adapter.InitLinkData(storeData.Title, storeData.Link)
	if err != nil {
		log.Printf(err.Error())
		http.Error(w, "Internal error", http.StatusInsufficientStorage)
		return
	}

	err = server.Producer.QueueLinkCopyJob(claimedId, storeData.Title, storeData.Link)
	if err == nil {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(IdBody{claimedId})

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
