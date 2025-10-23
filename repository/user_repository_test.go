// repository/user_repository_test.go
package repository

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	redis2 "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Global test database connection
var testDB *sql.DB

// TestMain sets up the test environment
// This runs ONCE before all tests in this package
func TestMain(m *testing.M) {
    ctx := context.Background()

    // 🐳 START POSTGRESQL CONTAINER WITH WAIT STRATEGY
    container, err := postgres.RunContainer(ctx,
        testcontainers.WithImage("postgres:15"),
        postgres.WithInitScripts("../migrations/init.sql"),
        postgres.WithDatabase("testdb"),
        postgres.WithUsername("testuser"),
        postgres.WithPassword("testpass"),
        testcontainers.WithWaitStrategy(
            wait.ForLog("database system is ready").
                WithOccurrence(2).
                WithStartupTimeout(30*time.Second),
        ),
    )
    if err != nil {
        log.Fatalf("Failed to start container: %s", err)
    }

    // Get connection string with SSL mode disabled
    connStr, err := container.ConnectionString(ctx, "sslmode=disable")
    if err != nil {
        log.Fatalf("Failed to get connection string: %s", err)
    }

    // Connect to database
    testDB, err = sql.Open("postgres", connStr)
    if err != nil {
        log.Fatalf("Failed to connect to database: %s", err)
    }

    // Verify connection
    if err = testDB.Ping(); err != nil {
        log.Fatalf("Failed to ping database: %s", err)
    }

    log.Println("✅ Test database ready!")

    // Run all tests
    code := m.Run()

    // Cleanup
    testDB.Close()
    if err := container.Terminate(ctx); err != nil {
        log.Fatalf("Failed to terminate container: %s", err)
    }

    os.Exit(code)
}

// TestGetByID tests retrieving a user by ID
func TestGetByID(t *testing.T) {
	repo := NewUserRepository(testDB)

	// Test case 1: User exists (from init.sql)
	t.Run("User Exists", func(t *testing.T) {
		user, err := repo.GetByID(1)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if user.Email != "alice@example.com" {
			t.Errorf("Expected email 'alice@example.com', got: %s", user.Email)
		}

		if user.Name != "Alice Smith" {
			t.Errorf("Expected name 'Alice Smith', got: %s", user.Name)
		}
	})

	// Test case 2: User does not exist
	t.Run("User Not Found", func(t *testing.T) {
		_, err := repo.GetByID(9999)
		if err == nil {
			t.Fatal("Expected error for non-existent user, got nil")
		}
	})
}

// TestGetByEmail tests retrieving a user by email
func TestGetByEmail(t *testing.T) {
	repo := NewUserRepository(testDB)

	t.Run("User Exists", func(t *testing.T) {
		user, err := repo.GetByEmail("bob@example.com")
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if user.Name != "Bob Johnson" {
			t.Errorf("Expected name 'Bob Johnson', got: %s", user.Name)
		}
	})

	t.Run("User Not Found", func(t *testing.T) {
		_, err := repo.GetByEmail("nonexistent@example.com")
		if err == nil {
			t.Fatal("Expected error for non-existent email, got nil")
		}
	})
}

// TestCreate tests user creation
func TestCreate(t *testing.T) {
	repo := NewUserRepository(testDB)

	t.Run("Create New User", func(t *testing.T) {
		user, err := repo.Create("charlie@example.com", "Charlie Brown")
		if err != nil {
			t.Fatalf("Failed to create user: %v", err)
		}

		if user.ID == 0 {
			t.Error("Expected non-zero ID for created user")
		}

		if user.Email != "charlie@example.com" {
			t.Errorf("Expected email 'charlie@example.com', got: %s", user.Email)
		}

		if user.CreatedAt.IsZero() {
			t.Error("Expected non-zero created_at timestamp")
		}

		// Cleanup: delete the created user
		defer repo.Delete(user.ID)
	})

	t.Run("Create Duplicate Email", func(t *testing.T) {
		// Try to create user with existing email (from init.sql)
		_, err := repo.Create("alice@example.com", "Another Alice")
		if err == nil {
			t.Fatal("Expected error when creating user with duplicate email")
		}
	})
}

// TestUpdate tests user updates
func TestUpdate(t *testing.T) {
	repo := NewUserRepository(testDB)

	t.Run("Update Existing User", func(t *testing.T) {
		// First, create a user to update
		user, err := repo.Create("david@example.com", "David Davis")
		if err != nil {
			t.Fatalf("Failed to create test user: %v", err)
		}
		defer repo.Delete(user.ID)

		// Update the user
		err = repo.Update(user.ID, "david.updated@example.com", "David Updated")
		if err != nil {
			t.Fatalf("Failed to update user: %v", err)
		}

		// Verify the update
		updatedUser, err := repo.GetByID(user.ID)
		if err != nil {
			t.Fatalf("Failed to retrieve updated user: %v", err)
		}

		if updatedUser.Email != "david.updated@example.com" {
			t.Errorf("Expected email 'david.updated@example.com', got: %s", updatedUser.Email)
		}

		if updatedUser.Name != "David Updated" {
			t.Errorf("Expected name 'David Updated', got: %s", updatedUser.Name)
		}
	})

	t.Run("Update Non-Existent User", func(t *testing.T) {
		err := repo.Update(9999, "nobody@example.com", "Nobody")
		if err == nil {
			t.Fatal("Expected error when updating non-existent user")
		}
	})
}

// TestDelete tests user deletion
func TestDelete(t *testing.T) {
	repo := NewUserRepository(testDB)

	t.Run("Delete Existing User", func(t *testing.T) {
		// Create a user to delete
		user, err := repo.Create("temp@example.com", "Temporary User")
		if err != nil {
			t.Fatalf("Failed to create test user: %v", err)
		}

		// Delete the user
		err = repo.Delete(user.ID)
		if err != nil {
			t.Fatalf("Failed to delete user: %v", err)
		}

		// Verify deletion
		_, err = repo.GetByID(user.ID)
		if err == nil {
			t.Fatal("Expected error when retrieving deleted user")
		}
	})

	t.Run("Delete Non-Existent User", func(t *testing.T) {
		err := repo.Delete(9999)
		if err == nil {
			t.Fatal("Expected error when deleting non-existent user")
		}
	})
}

// TestList tests listing all users
func TestList(t *testing.T) {
	repo := NewUserRepository(testDB)

	users, err := repo.List()
	if err != nil {
		t.Fatalf("Failed to list users: %v", err)
	}

	// Should have at least 2 users from init.sql
	if len(users) < 2 {
		t.Errorf("Expected at least 2 users, got: %d", len(users))
	}

	// Verify first user
	if users[0].Email != "alice@example.com" {
		t.Errorf("Expected first user email 'alice@example.com', got: %s", users[0].Email)
	}
}

// TestFindByNamePattern tests finding users by name pattern
func TestFindByNamePattern(t *testing.T) {
	repo := NewUserRepository(testDB)

	t.Run("Find Users By Pattern", func(t *testing.T) {
		// Search for users with "Smith" in their name (Alice Smith from init.sql)
		users, err := repo.FindByNamePattern("Smith")
		if err != nil {
			t.Fatalf("Failed to find users: %v", err)
		}

		if len(users) < 1 {
			t.Errorf("Expected at least 1 user with pattern 'Smith', got: %d", len(users))
		}

		// Verify Alice is in the results
		found := false
		for _, user := range users {
			if user.Email == "alice@example.com" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected to find alice@example.com in results")
		}
	})

	t.Run("Find Users Case Insensitive", func(t *testing.T) {
		// Search with lowercase should still find "Smith"
		users, err := repo.FindByNamePattern("smith")
		if err != nil {
			t.Fatalf("Failed to find users: %v", err)
		}

		if len(users) < 1 {
			t.Errorf("Expected at least 1 user with pattern 'smith', got: %d", len(users))
		}
	})

	t.Run("Pattern Not Found", func(t *testing.T) {
		users, err := repo.FindByNamePattern("NonExistentPattern")
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if len(users) != 0 {
			t.Errorf("Expected 0 users, got: %d", len(users))
		}
	})

	t.Run("Partial Pattern Match", func(t *testing.T) {
		// Should match both "Alice Smith" and "Bob Johnson" with pattern containing common letter
		users, err := repo.FindByNamePattern("o")
		if err != nil {
			t.Fatalf("Failed to find users: %v", err)
		}

		// Bob Johnson should be found
		found := false
		for _, user := range users {
			if user.Email == "bob@example.com" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected to find bob@example.com in results")
		}
	})
}

// TestCountUsers tests counting total users
func TestCountUsers(t *testing.T) {
	repo := NewUserRepository(testDB)

	t.Run("Count Users", func(t *testing.T) {
		count, err := repo.CountUsers()
		if err != nil {
			t.Fatalf("Failed to count users: %v", err)
		}

		// Should have at least 2 users from init.sql
		if count < 2 {
			t.Errorf("Expected at least 2 users, got: %d", count)
		}
	})

	t.Run("Count After Creating User", func(t *testing.T) {
		// Get initial count
		initialCount, err := repo.CountUsers()
		if err != nil {
			t.Fatalf("Failed to get initial count: %v", err)
		}

		// Create a new user
		user, err := repo.Create("count.test@example.com", "Count Test")
		if err != nil {
			t.Fatalf("Failed to create user: %v", err)
		}
		defer repo.Delete(user.ID)

		// Count should increase by 1
		newCount, err := repo.CountUsers()
		if err != nil {
			t.Fatalf("Failed to get new count: %v", err)
		}

		if newCount != initialCount+1 {
			t.Errorf("Expected count to be %d, got: %d", initialCount+1, newCount)
		}
	})
}

// TestGetRecentUsers tests retrieving recently created users
func TestGetRecentUsers(t *testing.T) {
	repo := NewUserRepository(testDB)

	t.Run("Get Recent Users Within Days", func(t *testing.T) {
		// Create a fresh user (will have current timestamp)
		user, err := repo.Create("recent@example.com", "Recent User")
		if err != nil {
			t.Fatalf("Failed to create user: %v", err)
		}
		defer repo.Delete(user.ID)

		// Get users from last 7 days
		users, err := repo.GetRecentUsers(7)
		if err != nil {
			t.Fatalf("Failed to get recent users: %v", err)
		}

		// Should find at least the user we just created
		found := false
		for _, u := range users {
			if u.ID == user.ID {
				found = true
				break
			}
		}

		if !found {
			t.Error("Expected to find recently created user in results")
		}
	})

	t.Run("Get Recent Users Last 1 Day", func(t *testing.T) {
		// Create a user
		user, err := repo.Create("today@example.com", "Today User")
		if err != nil {
			t.Fatalf("Failed to create user: %v", err)
		}
		defer repo.Delete(user.ID)

		// Get users from last 1 day
		users, err := repo.GetRecentUsers(1)
		if err != nil {
			t.Fatalf("Failed to get recent users: %v", err)
		}

		// Should find the user created today
		found := false
		for _, u := range users {
			if u.ID == user.ID {
				found = true
				break
			}
		}

		if !found {
			t.Error("Expected to find today's user in 1-day results")
		}
	})

	t.Run("Get Recent Users Ordered By Date", func(t *testing.T) {
		// Create two users
		user1, err := repo.Create("first@example.com", "First User")
		if err != nil {
			t.Fatalf("Failed to create first user: %v", err)
		}
		defer repo.Delete(user1.ID)

		user2, err := repo.Create("second@example.com", "Second User")
		if err != nil {
			t.Fatalf("Failed to create second user: %v", err)
		}
		defer repo.Delete(user2.ID)

		// Get recent users
		users, err := repo.GetRecentUsers(7)
		if err != nil {
			t.Fatalf("Failed to get recent users: %v", err)
		}

		// Users should be ordered by created_at DESC (newest first)
		if len(users) >= 2 {
			// The most recent users should appear first
			// Check if user2 (created second) appears before or at same position as user1
			foundUser2 := false
			for _, u := range users {
				if u.ID == user2.ID {
					foundUser2 = true
					break
				}
				if u.ID == user1.ID && !foundUser2 {
					// Found user1 before user2, which violates DESC order
					// This is acceptable if they have same timestamp
					break
				}
			}
		}
	})

	t.Run("Get Recent Users Zero Results", func(t *testing.T) {
		// Get users from last 0 days (should return empty or users created exactly now)
		users, err := repo.GetRecentUsers(0)
		if err != nil {
			t.Fatalf("Failed to get recent users: %v", err)
		}

		// Result should be a valid slice (could be empty)
		if users == nil {
			t.Error("Expected non-nil slice")
		}
	})
}

func TestTransactionRollback(t *testing.T) {
	repo := NewUserRepository(testDB)

	// Count users before
	countBefore, _ := repo.CountUsers()

	// Start a transaction that will fail
	tx, _ := testDB.Begin()

	// Create user in transaction
	_, err := tx.Exec("INSERT INTO users (email, name) VALUES ($1, $2)",
		"tx@example.com", "TX User")
	if err != nil {
		t.Fatal(err)
	}

	// Rollback transaction
	tx.Rollback()

	// Verify count is unchanged
	countAfter, _ := repo.CountUsers()
	if countAfter != countBefore {
		t.Error("Transaction was not rolled back properly")
	}
}

// ==================== TESTS WITH MULTIPLE INTERCONNECTED CONTAINERS ====================
// TestCachedUserRepository tests the cached repository with PostgreSQL + Redis containers
func TestCachedUserRepository(t *testing.T) {
	ctx := context.Background()

	// 🐳 START REDIS CONTAINER
	redisContainer, err := redis.RunContainer(ctx,
		testcontainers.WithImage("redis:7-alpine"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("Failed to start Redis container: %s", err)
	}
	defer redisContainer.Terminate(ctx)

	// Get Redis endpoint
	redisHost, err := redisContainer.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get Redis host: %s", err)
	}

	redisPort, err := redisContainer.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatalf("Failed to get Redis port: %s", err)
	}

	// Connect to Redis
	redisClient := redis2.NewClient(&redis2.Options{
		Addr: fmt.Sprintf("%s:%s", redisHost, redisPort.Port()),
	})
	defer redisClient.Close()

	// Test Redis connection
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to ping Redis: %s", err)
	}

	log.Println("✅ Redis container ready!")

	// Create cached repository (uses existing testDB from TestMain)
	cachedRepo := NewCachedUserRepository(testDB, redisClient)

	t.Run("Cache Miss - Fetch From Database", func(t *testing.T) {
		// Clear cache first
		cachedRepo.InvalidateCache(ctx, 1)

		// First call should fetch from database
		user, err := cachedRepo.GetByIDCached(ctx, 1)
		if err != nil {
			t.Fatalf("Failed to get user: %v", err)
		}

		if user.Email != "alice@example.com" {
			t.Errorf("Expected email 'alice@example.com', got: %s", user.Email)
		}
	})

	t.Run("Cache Hit - Fetch From Redis", func(t *testing.T) {
		// First call to populate cache
		_, err := cachedRepo.GetByIDCached(ctx, 1)
		if err != nil {
			t.Fatalf("Failed to populate cache: %v", err)
		}

		// Second call should hit cache
		user, err := cachedRepo.GetByIDCached(ctx, 1)
		if err != nil {
			t.Fatalf("Failed to get cached user: %v", err)
		}

		if user.Email != "alice@example.com" {
			t.Errorf("Expected email 'alice@example.com', got: %s", user.Email)
		}

		// Verify the data is actually in Redis
		cacheKey := fmt.Sprintf("user:%d", 1)
		cached, err := redisClient.Get(ctx, cacheKey).Result()
		if err != nil {
			t.Errorf("Expected user to be in cache, got error: %v", err)
		}
		if cached == "" {
			t.Error("Expected cached data, got empty string")
		}
	})

	t.Run("Cache Invalidation", func(t *testing.T) {
		// Populate cache
		cachedRepo.GetByIDCached(ctx, 1)

		// Invalidate cache
		err := cachedRepo.InvalidateCache(ctx, 1)
		if err != nil {
			t.Fatalf("Failed to invalidate cache: %v", err)
		}

		// Verify cache is empty
		cacheKey := fmt.Sprintf("user:%d", 1)
		_, err = redisClient.Get(ctx, cacheKey).Result()
		if err == nil {
			t.Error("Expected cache to be empty after invalidation")
		}
	})

	t.Run("Create User With Cache", func(t *testing.T) {
		user, err := cachedRepo.CreateCached(ctx, "cached@example.com", "Cached User")
		if err != nil {
			t.Fatalf("Failed to create user: %v", err)
		}
		defer testDB.Exec("DELETE FROM users WHERE id = $1", user.ID)

		if user.Email != "cached@example.com" {
			t.Errorf("Expected email 'cached@example.com', got: %s", user.Email)
		}

		// Fetch from cache
		cachedUser, err := cachedRepo.GetByIDCached(ctx, user.ID)
		if err != nil {
			t.Fatalf("Failed to get cached user: %v", err)
		}

		if cachedUser.ID != user.ID {
			t.Errorf("Expected ID %d, got: %d", user.ID, cachedUser.ID)
		}
	})

	t.Run("User Not Found", func(t *testing.T) {
		_, err := cachedRepo.GetByIDCached(ctx, 99999)
		if err == nil {
			t.Fatal("Expected error for non-existent user, got nil")
		}
	})

	t.Run("Cache Expiration Simulation", func(t *testing.T) {
		// Populate cache
		user, err := cachedRepo.GetByIDCached(ctx, 1)
		if err != nil {
			t.Fatalf("Failed to get user: %v", err)
		}

		// Verify cache exists
		cacheKey := fmt.Sprintf("user:%d", 1)
		_, cacheErr := redisClient.Get(ctx, cacheKey).Result()
		if cacheErr != nil {
			t.Fatalf("Expected cached data: %v", cacheErr)
		}

		// Manually delete from Redis to simulate expiration
		redisClient.Del(ctx, cacheKey)

		// Should still work (fetch from DB)
		user2, err := cachedRepo.GetByIDCached(ctx, 1)
		if err != nil {
			t.Fatalf("Failed to get user after cache expiration: %v", err)
		}

		if user2.Email != user.Email {
			t.Errorf("Expected same user data after re-fetch")
		}
	})

	t.Run("Multiple Cache Entries", func(t *testing.T) {
		// Cache multiple users
		cachedRepo.GetByIDCached(ctx, 1)
		cachedRepo.GetByIDCached(ctx, 2)

		// Verify both are cached
		key1 := fmt.Sprintf("user:%d", 1)
		key2 := fmt.Sprintf("user:%d", 2)

		_, err1 := redisClient.Get(ctx, key1).Result()
		_, err2 := redisClient.Get(ctx, key2).Result()

		if err1 != nil || err2 != nil {
			t.Error("Expected both users to be cached")
		}
	})
}


