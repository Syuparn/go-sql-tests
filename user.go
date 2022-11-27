package gosqltests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/samber/lo"
	"github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"

	"github.com/syuparn/gosqltests/models"
)

type User struct {
	ID   string
	Name string
	Age  int
}

type userRepository struct {
	db *sql.DB
}

func NewUserRepository(db *sql.DB) *userRepository {
	return &userRepository{
		db: db,
	}
}

func (r *userRepository) Register(ctx context.Context, user *User) error {
	c := &models.User{
		ID:   user.ID,
		Name: user.Name,
		Age:  null.IntFrom(user.Age),
	}

	if err := c.Insert(ctx, r.db, boil.Infer()); err != nil {
		return fmt.Errorf("failed to insert user: %w", err)
	}

	return nil
}

func (r *userRepository) List(ctx context.Context) ([]*User, error) {
	users, err := models.Users().All(ctx, r.db)
	if err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
	}

	return lo.Map(users, func(c *models.User, _ int) *User {
		return &User{
			ID:   c.ID,
			Name: c.Name,
			Age:  c.Age.Int,
		}
	}), nil
}

func (r *userRepository) Get(ctx context.Context, id string) (*User, error) {
	user, err := models.Users(
		models.UserWhere.ID.EQ(string(id)),
	).One(ctx, r.db)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("user was not found (id: %s): %w", id, err)
		}

		return nil, fmt.Errorf("failed to get user (id: %s): %w", id, err)
	}

	return &User{
		ID:   user.ID,
		Name: user.Name,
		Age:  user.Age.Int,
	}, nil
}

func (r *userRepository) Delete(ctx context.Context, user *User) error {
	c := &models.User{
		ID:   string(user.ID),
		Name: string(user.Name),
	}

	if _, err := c.Delete(ctx, r.db); err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	return nil
}
