package gosqltests

import (
	"database/sql"
	"fmt"

	// NOTE: used for mysql client plugin
	_ "github.com/go-sql-driver/mysql"
)

func NewClient(port int) (*sql.DB, error) {
	// TODO: make this configurable
	db, err := sql.Open("mysql", fmt.Sprintf("root:@(localhost:%d)/practice", port))
	if err != nil {
		return nil, fmt.Errorf("failed to create MySQL client: %w", err)
	}
	return db, nil
}
