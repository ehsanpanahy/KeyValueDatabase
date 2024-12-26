package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	store "example.com/gorilla/store"
	"example.com/gorilla/transaction"
	postgres "example.com/gorilla/transaction/postgres"
	"github.com/gorilla/mux"
)

var logger transaction.TransactionLogger

func initializeTransactionLog() error {
	var err error

	logger, err = postgres.NewPostgresTransactionLogger(postgres.PostgressDBParams{
		Host:     "localhost",
		DbName:   "kvs",
		User:     "test",
		Password: "hunter3",
	})

	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := logger.ReadEvents()

	e, ok := transaction.Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case transaction.EventDelete:
				err = store.Delete(e.Key)
			case transaction.EventPut:
				err = store.Put(e.Key, e.Value)
			}
		}
	}

	logger.Run()

	return err
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := store.Get(key)
	if errors.Is(err, store.ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))
}

func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := store.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	logger.WriteDelete(key)

	w.WriteHeader(http.StatusOK)
}

// keyValuePutHandler expects to be called with a PUT request for
// the "/v1/key/{key}" resource.
func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = store.Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	logger.WritePut(key, string(value))

	w.WriteHeader(http.StatusCreated)
}

func helloMuxHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello gorilla/mux!\n"))
}

func main() {
	// Create a new mux router
	r := mux.NewRouter()

	err := initializeTransactionLog()
	if err != nil {
		fmt.Errorf("There was an error in initializing Transaction Logger: %w", err)
		return
	}

	// Associate a path with a handler function on the router
	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods("DELETE")

	// Bind to a port and pass in the mux router
	log.Fatal(http.ListenAndServe(":8080", r))
}
