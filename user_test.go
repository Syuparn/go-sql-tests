package gosqltests

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// test using docker container
func TestListWithDocker(t *testing.T) {
	ctx := context.Background()
	user := &User{
		ID:   "0123456789ABCDEFGHJKMNPQRS",
		Name: "Mike",
		Age:  20,
	}

	db, err := NewClient(3306)
	require.NoError(t, err)

	// run
	r := NewUserRepository(db)
	err = r.Register(ctx, user)
	require.NoError(t, err)

	// teardown
	defer r.Delete(ctx, user)

	found, err := r.Get(ctx, user.ID)
	require.NoError(t, err)

	require.Equal(t, user, found)
}

// test using testcontainers
func TestGetWithTestContainers(t *testing.T) {
	ctx := context.Background()
	user := &User{
		ID:   "0123456789ABCDEFGHJKMNPQRS",
		Name: "Mike",
		Age:  20,
	}

	db, teardown := prepareContainer(ctx, t)
	defer teardown()

	// run
	r := NewUserRepository(db)
	err := r.Register(ctx, user)
	require.NoError(t, err)

	found, err := r.Get(ctx, user.ID)
	require.NoError(t, err)

	require.Equal(t, user, found)
}

func prepareContainer(ctx context.Context, t *testing.T) (*sql.DB, func()) {
	req := testcontainers.ContainerRequest{
		Image: "mysql:8",
		Env: map[string]string{
			"MYSQL_ALLOW_EMPTY_PASSWORD": "yes",
			"MYSQL_DATABASE":             "practice",
		},
		ExposedPorts: []string{"3306/tcp"},
		Mounts: testcontainers.ContainerMounts{
			testcontainers.BindMount(absPath("initdb.d"), "/docker-entrypoint-initdb.d"),
		},
		WaitingFor: wait.ForSQL("3306/tcp", "mysql", func(host string, port nat.Port) string {
			return fmt.Sprintf("root:@(%s:%d)/practice", host, port.Int())
		}),
		AutoRemove: true,
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start container: %s", err)
	}

	teardown := func() {
		if err := container.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}

	port, err := container.MappedPort(ctx, "3306")
	if err != nil {
		t.Fatalf("failed to get mapped port: %s", err)
	}

	db, err := NewClient(port.Int())
	if err != nil {
		t.Fatalf("failed to create client: %s", err)
	}

	return db, teardown
}

func absPath(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		panic(err)
	}

	return abs
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
