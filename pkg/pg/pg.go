package pg

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func escp(p string) string {
	return strings.NewReplacer("=", "\\=", " ", "__").Replace(p)
}

type pgOptions struct {
	db       string
	user     string
	password string

	dialAttempts int
}

// PGOpt sets an option for the opener.
type PGOpt func(opts *pgOptions)

// WithDialAttempts allows the connection to be attempted multiple times before finally erroring out.
func WithDialAttempts(num int) PGOpt {
	if num < 1 {
		num = 1
	}
	return func(opts *pgOptions) {
		opts.dialAttempts = num
	}
}

// WithUsername changes the username this database will use to connect.
func WithUsername(name string) PGOpt {
	return func(opts *pgOptions) {
		opts.user = name
	}
}

// WithPassword sets the connection password.
func WithPassword(pwd string) PGOpt {
	return func(opts *pgOptions) {
		opts.password = pwd
	}
}

// WithDB changes the name of the database to connect to.
func WithDB(db string) PGOpt {
	return func(opts *pgOptions) {
		opts.db = db
	}
}

// Open opens a PostgreSQL database at the given host, returning an sql.DB.
func Open(ctx context.Context, addr string, opts ...PGOpt) (*sql.DB, error) {
	options := &pgOptions{
		db:           "postgres",
		user:         "postgres",
		password:     "",
		dialAttempts: 5,
	}
	for _, o := range opts {
		o(options)
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		escp(options.user),
		escp(options.password),
		addr,
		escp(options.db))

	var err error
	for i := 0; i < options.dialAttempts; i++ {
		log.Printf("Attempting connection %d of %d to %v", i+1, options.dialAttempts, addr)
		var db *sql.DB
		if db, err = connect(ctx, connStr); err == nil {
			log.Printf("Connected")
			return db, nil
		}
		log.Printf("Error connecting: %v", err)
		if i < options.dialAttempts-1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
	}
	return nil, err
}

// connect tries to open and connect to a postgres database.
func connect(ctx context.Context, config string) (*sql.DB, error) {
	db, err := sql.Open("postgres", config)
	if err != nil {
		return nil, fmt.Errorf("connect open: %v", err)
	}
	if _, err := db.ExecContext(ctx, `SELECT 1`); err != nil {
		return nil, fmt.Errorf("connect select: %v", err)
	}
	return db, nil
}
