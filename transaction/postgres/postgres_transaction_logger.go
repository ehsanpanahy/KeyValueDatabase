package postgres

import (
	"database/sql"
	"fmt"

	transaction "example.com/gorilla/transaction"

	_ "github.com/lib/pq"
)

type PostgressDBParams struct {
	DbName   string
	Host     string
	User     string
	Password string
}

const TableName = "transactions"

type PostgresTransactionLogger struct {
	events chan<- transaction.Event
	errors <-chan error
	db     *sql.DB
}

func NewPostgresTransactionLogger(config PostgressDBParams) (transaction.TransactionLogger, error) {

	connStr := fmt.Sprintf("host=%s dbname=%s user=%s password=%s",
		config.Host, config.DbName, config.User, config.Password)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("Failed to open db: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	logger := &PostgresTransactionLogger{db: db}

	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}

	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	query := fmt.Sprintf(`IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES
					WHERE TABLE_NAME = '%s'))`, TableName)

	var exists bool

	err := l.db.QueryRow(query).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return false, fmt.Errorf("error checking if table exists: %w", err)
	}

	return exists, nil
}

func (l *PostgresTransactionLogger) createTable() error {
	query := `CREATE TABLE %s (
		sequence BIGSERIAL PRIMARY KEY,
		event_type BYTE NOT NULL,
		key TEXT NOT NULL,
		value TEXT
	)`

	_, err := l.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func (l *PostgresTransactionLogger) Run() {
	events := make(chan transaction.Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		query := `INSERT INTO transations (event_type, key, value) VALUES ($1, $2, $3)`
		for e := range events {
			_, err := l.db.Exec(query, e.EventType, e.Key, e.Value)

			if err != nil {
				errors <- err
			}
		}
	}()
}

func (l *PostgresTransactionLogger) ReadEvents() (<-chan transaction.Event, <-chan error) {
	outEvent := make(chan transaction.Event)
	outError := make(chan error)

	go func() {
		defer close(outEvent)
		defer close(outError)

		query := `SELECT sequence, event_type, key, value FROM transactions
					ORDER BY sequence`

		rows, err := l.db.Query(query)
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}

		defer rows.Close()

		e := transaction.Event{}

		for rows.Next() {

			err = rows.Scan(&e.Sequence, &e.EventType, &e.Key, &e.Value)
			if err != nil {
				outError <- fmt.Errorf("error reading row: %w", err)
				return
			}

			outEvent <- e
		}

		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
	}()

	return outEvent, outError
}

func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.events <- transaction.Event{EventType: transaction.EventPut, Key: key, Value: value}
}

func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- transaction.Event{EventType: transaction.EventDelete, Key: key}
}

func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}
