package gosqltests

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// test using docker container
func TestGetWithDocker(t *testing.T) {

}

// test using test container
func TestGetWithTestContainer(t *testing.T) {

}

// test using go-sqlmock
func TestGetWithSQLMock(t *testing.T) {
	columns := []string{"id", "name", "age"}

	tests := []struct {
		title    string
		id       string
		query    string
		mockRow  []driver.Value
		expected *User
	}{
		{
			"get a user",
			"0123456789ABCDEFGHJKMNPQRS",
			"SELECT `user`.* FROM `user` WHERE (`user`.`id` = ?) LIMIT 1",
			[]driver.Value{"0123456789ABCDEFGHJKMNPQRS", "Mike", 20},
			&User{
				ID:   "0123456789ABCDEFGHJKMNPQRS",
				Name: "Mike",
				Age:  20,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.title, func(t *testing.T) {
			// mock
			db, mock, teardown := prepareMockDB(t)
			defer teardown()
			rows := sqlmock.NewRows(columns).AddRow(tt.mockRow...)
			mock.ExpectQuery(regexp.QuoteMeta(tt.query)).
				WillReturnRows(rows)

			// run
			r := NewUserRepository(db)
			actual, err := r.Get(context.TODO(), tt.id)

			// assert
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func prepareMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock, func()) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	teardown := func() {
		db.Close()
	}

	return db, mock, teardown
}
