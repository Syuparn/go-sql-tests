package gosqltests

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/docker/go-connections/nat"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	simsql "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
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

func TestGetWithTestContainersConcurrent(t *testing.T) {
	tests := []struct {
		title string
		user  *User
	}{
		{
			"user Mike",
			&User{
				ID:   "0123456789ABCDEFGHJKMNPQRS",
				Name: "Mike",
				Age:  20,
			},
		},
		{
			"user Bob",
			&User{
				ID:   "1123456789ABCDEFGHJKMNPQRS",
				Name: "Bob",
				Age:  25,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.title, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db, teardown := prepareContainer(ctx, t)
			defer teardown()

			// run
			r := NewUserRepository(db)
			err := r.Register(ctx, tt.user)
			require.NoError(t, err)

			found, err := r.Get(ctx, tt.user.ID)
			require.NoError(t, err)

			require.Equal(t, tt.user, found)
		})
	}
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

func TestGetErrorWithSQLMock(t *testing.T) {
	tests := []struct {
		title       string
		id          string
		query       string
		mockErr     error
		expectedErr string
	}{
		{
			"not found",
			"0123456789ABCDEFGHJKMNPQRS",
			"SELECT `user`.* FROM `user` WHERE (`user`.`id` = ?) LIMIT 1",
			sql.ErrNoRows,
			"user was not found (id: 0123456789ABCDEFGHJKMNPQRS): sql: no rows in result set",
		},
		{
			"unexpected error",
			"0123456789ABCDEFGHJKMNPQRS",
			"SELECT `user`.* FROM `user` WHERE (`user`.`id` = ?) LIMIT 1",
			fmt.Errorf("crashed unexpectedly!!!"),
			"failed to get user (id: 0123456789ABCDEFGHJKMNPQRS): models: failed to execute a one query for user: bind failed to execute query: crashed unexpectedly!!!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.title, func(t *testing.T) {
			// mock
			db, mock, teardown := prepareMockDB(t)
			defer teardown()
			mock.ExpectQuery(regexp.QuoteMeta(tt.query)).
				WillReturnError(tt.mockErr)

			// run
			r := NewUserRepository(db)
			_, err := r.Get(context.TODO(), tt.id)

			// assert
			require.Error(t, err)
			require.EqualError(t, err, tt.expectedErr)
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

// test using go-mysql-server
func TestGetWithGoMySQLServer(t *testing.T) {
	tests := []struct {
		title    string
		id       string
		prepare  func(*simsql.Context, *memory.Table)
		expected *User
	}{
		{
			"get a user",
			"0123456789ABCDEFGHJKMNPQRS",
			func(ctx *simsql.Context, table *memory.Table) {
				_ = table.Insert(ctx, simsql.NewRow(
					"0123456789ABCDEFGHJKMNPQRS",
					"Mike",
					int64(20),
				))
				_ = table.Insert(ctx, simsql.NewRow(
					"1123456789ABCDEFGHJKMNPQRS",
					"Bob",
					int64(25),
				))
			},
			&User{
				ID:   "0123456789ABCDEFGHJKMNPQRS",
				Name: "Mike",
				Age:  20,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.title, func(t *testing.T) {
			// simulator
			table, teardown := prepareSimulator(t, 23306)
			defer teardown()
			tt.prepare(simsql.NewEmptyContext(), table)

			// run
			db, err := NewClient(23306)
			require.NoError(t, err)
			r := NewUserRepository(db)
			actual, err := r.Get(context.TODO(), tt.id)

			// assert
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestGetWithGoMySQLServerConcurrent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		title    string
		id       string
		prepare  func(*simsql.Context, *memory.Table)
		expected *User
	}{
		{
			"get Mike",
			"0123456789ABCDEFGHJKMNPQRS",
			func(ctx *simsql.Context, table *memory.Table) {
				_ = table.Insert(ctx, simsql.NewRow(
					"0123456789ABCDEFGHJKMNPQRS",
					"Mike",
					int64(20),
				))
				_ = table.Insert(ctx, simsql.NewRow(
					"1123456789ABCDEFGHJKMNPQRS",
					"Bob",
					int64(25),
				))
			},
			&User{
				ID:   "0123456789ABCDEFGHJKMNPQRS",
				Name: "Mike",
				Age:  20,
			},
		},
		{
			"get Bob",
			"1123456789ABCDEFGHJKMNPQRS",
			func(ctx *simsql.Context, table *memory.Table) {
				_ = table.Insert(ctx, simsql.NewRow(
					"0123456789ABCDEFGHJKMNPQRS",
					"Mike",
					int64(20),
				))
				_ = table.Insert(ctx, simsql.NewRow(
					"1123456789ABCDEFGHJKMNPQRS",
					"Bob",
					int64(25),
				))
			},
			&User{
				ID:   "1123456789ABCDEFGHJKMNPQRS",
				Name: "Bob",
				Age:  25,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.title, func(t *testing.T) {
			t.Parallel()

			// simulator
			port, err := freePort()
			require.NoError(t, err)
			table, teardown := prepareSimulator(t, port)
			defer teardown()
			tt.prepare(simsql.NewEmptyContext(), table)

			// run
			db, err := NewClient(port)
			require.NoError(t, err)
			r := NewUserRepository(db)
			actual, err := r.Get(context.TODO(), tt.id)

			// assert
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func freePort() (int, error) {
	// NOTE: free port are chosen if port 0 is specified
	l, err := net.Listen("tcp4", "localhost:0")
	if err != nil {
		return 0, err
	}
	// close connection to use later
	l.Close()
	addr := l.Addr().(*net.TCPAddr)
	return addr.Port, nil
}

func prepareSimulator(t *testing.T, port int) (*memory.Table, func()) {
	db, table := simulatorDB()

	engine := sqle.NewDefault(
		simsql.NewDatabaseProvider(
			db,
			information_schema.NewInformationSchemaDatabase(),
		))
	engine.Analyzer.Catalog.MySQLDb.AddSuperUser("root", "localhost", "")

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("localhost:%d", port),
	}
	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		if err = s.Start(); err != nil {
			panic(err)
		}
	}()

	teardown := func() {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}

	return table, teardown
}

func simulatorDB() (*memory.Database, *memory.Table) {
	db := memory.NewDatabase("practice")

	tableName := "user"
	table := memory.NewTable(tableName, simsql.NewPrimaryKeySchema(simsql.Schema{
		{Name: "id", Type: simsql.Text, Nullable: false, Source: tableName, PrimaryKey: true},
		{Name: "name", Type: simsql.Text, Nullable: false, Source: tableName},
		{Name: "age", Type: simsql.Int64, Nullable: false, Source: tableName},
	}), db.GetForeignKeyCollection())
	db.AddTable(tableName, table)

	return db, table
}
