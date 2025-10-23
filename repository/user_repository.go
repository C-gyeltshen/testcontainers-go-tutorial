// repository/user_repository.go
package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"testcontainers-demo/models"

	"github.com/redis/go-redis/v9"
)

// UserRepository handles database operations for users
type UserRepository struct {
	db *sql.DB
}



// NewUserRepository creates a new user repository
func NewUserRepository(db *sql.DB) *UserRepository {
	return &UserRepository{db: db}
}

// GetByID retrieves a user by their ID
func (r *UserRepository) GetByID(id int) (*models.User, error) {
	query := "SELECT id, email, name, created_at FROM users WHERE id = $1"

	var user models.User
	err := r.db.QueryRow(query, id).Scan(
		&user.ID,
		&user.Email,
		&user.Name,
		&user.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("user not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	return &user, nil
}

// GetByEmail retrieves a user by their email
func (r *UserRepository) GetByEmail(email string) (*models.User, error) {
	query := "SELECT id, email, name, created_at FROM users WHERE email = $1"

	var user models.User
	err := r.db.QueryRow(query, email).Scan(
		&user.ID,
		&user.Email,
		&user.Name,
		&user.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("user not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	return &user, nil
}

// Create inserts a new user
func (r *UserRepository) Create(email, name string) (*models.User, error) {
	query := `
		INSERT INTO users (email, name)
		VALUES ($1, $2)
		RETURNING id, email, name, created_at
	`

	var user models.User
	err := r.db.QueryRow(query, email, name).Scan(
		&user.ID,
		&user.Email,
		&user.Name,
		&user.CreatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	return &user, nil
}

// Update modifies an existing user
func (r *UserRepository) Update(id int, email, name string) error {
	query := "UPDATE users SET email = $1, name = $2 WHERE id = $3"

	result, err := r.db.Exec(query, email, name, id)
	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("user not found")
	}

	return nil
}

// Delete removes a user
func (r *UserRepository) Delete(id int) error {
	query := "DELETE FROM users WHERE id = $1"

	result, err := r.db.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("user not found")
	}

	return nil
}

// List retrieves all users
func (r *UserRepository) List() ([]models.User, error) {
	query := "SELECT id, email, name, created_at FROM users ORDER BY id"

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
	}
	defer rows.Close()

	var users []models.User
	for rows.Next() {
		var user models.User
		err := rows.Scan(&user.ID, &user.Email, &user.Name, &user.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan user: %w", err)
		}
		users = append(users, user)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating users: %w", err)
	}

	return users, nil
}

// FindByNamePattern finds users whose name matches a pattern
func (r *UserRepository) FindByNamePattern(pattern string) ([]models.User, error) {
	query := "SELECT id, email, name, created_at FROM users WHERE name ILIKE $1 ORDER BY id"

	rows, err := r.db.Query(query, "%"+pattern+"%")
	if err != nil {
		return nil, fmt.Errorf("failed to find users by pattern: %w", err)
	}
	defer rows.Close()

	users := []models.User{} // Initialize empty slice instead of nil
	for rows.Next() {
		var user models.User
		err := rows.Scan(&user.ID, &user.Email, &user.Name, &user.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan user: %w", err)
		}
		users = append(users, user)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating users: %w", err)
	}

	return users, nil
}

// CountUsers returns total number of users
func (r *UserRepository) CountUsers() (int, error) {
	query := "SELECT COUNT(*) FROM users"

	var count int
	err := r.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count users: %w", err)
	}

	return count, nil
}

// GetRecentUsers returns users created in the last N days
func (r *UserRepository) GetRecentUsers(days int) ([]models.User, error) {
	query := `
		SELECT id, email, name, created_at 
		FROM users 
		WHERE created_at >= NOW() - INTERVAL '1 day' * $1
		ORDER BY created_at DESC
	`

	rows, err := r.db.Query(query, days)
	if err != nil {
		return nil, fmt.Errorf("failed to get recent users: %w", err)
	}
	defer rows.Close()

	users := []models.User{} // Initialize empty slice instead of nil
	for rows.Next() {
		var user models.User
		err := rows.Scan(&user.ID, &user.Email, &user.Name, &user.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan user: %w", err)
		}
		users = append(users, user)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating users: %w", err)
	}

	return users, nil
}

// ==================== CACHED USER REPOSITORY ====================
// CachedUserRepository handles database operations with Redis caching
type CachedUserRepository struct {
	db    *sql.DB
	cache *redis.Client
}

// NewCachedUserRepository creates a new cached user repository
func NewCachedUserRepository(db *sql.DB, cache *redis.Client) *CachedUserRepository {
	return &CachedUserRepository{
		db:    db,
		cache: cache,
	}
}

// GetByIDCached retrieves a user by ID with caching
func (r *CachedUserRepository) GetByIDCached(ctx context.Context, id int) (*models.User, error) {
	// Try cache first
	cacheKey := fmt.Sprintf("user:%d", id)
	cached, err := r.cache.Get(ctx, cacheKey).Result()
	if err == nil {
		var user models.User
		if err := json.Unmarshal([]byte(cached), &user); err == nil {
			return &user, nil
		}
	}

	// Cache miss - query database
	user, err := r.getFromDB(id)
	if err != nil {
		return nil, err
	}

	// Store in cache
	data, _ := json.Marshal(user)
	r.cache.Set(ctx, cacheKey, data, 5*time.Minute)

	return user, nil
}

// getFromDB is a helper method to query user from database
func (r *CachedUserRepository) getFromDB(id int) (*models.User, error) {
	query := "SELECT id, email, name, created_at FROM users WHERE id = $1"

	var user models.User
	err := r.db.QueryRow(query, id).Scan(
		&user.ID,
		&user.Email,
		&user.Name,
		&user.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("user not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	return &user, nil
}

// InvalidateCache removes a user from the cache
func (r *CachedUserRepository) InvalidateCache(ctx context.Context, id int) error {
	cacheKey := fmt.Sprintf("user:%d", id)
	return r.cache.Del(ctx, cacheKey).Err()
}

// CreateCached creates a user and invalidates cache
func (r *CachedUserRepository) CreateCached(ctx context.Context, email, name string) (*models.User, error) {
	query := `
		INSERT INTO users (email, name)
		VALUES ($1, $2)
		RETURNING id, email, name, created_at
	`

	var user models.User
	err := r.db.QueryRow(query, email, name).Scan(
		&user.ID,
		&user.Email,
		&user.Name,
		&user.CreatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	return &user, nil
}




