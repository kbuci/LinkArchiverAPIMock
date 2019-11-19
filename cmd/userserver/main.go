package main

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/kbuci/text-hosting-mock/userserver"
)

func healthcheck(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello world"))
}

func main() {
	server := userserver.NewRequestServer()
	defer server.Close()
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/store/text", server.StoreData).Methods("POST")
	router.HandleFunc("/store/link", server.InitLinkData).Methods("POST")
	router.HandleFunc("/read/text/{id}", server.GetTextData).Methods("GET")
	router.HandleFunc("/read/link/{id}", server.GetArchivedLink).Methods("GET")
	router.HandleFunc("/healthcheck", healthcheck).Methods("GET")
	log.Print("starting server")
	log.Print(http.ListenAndServe(":8080", router))
}
