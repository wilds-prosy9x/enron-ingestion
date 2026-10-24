// Package redistest runs a Redis container shared by a package's tests.
package redistest

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// Image is the Redis image the tests run against.
const Image = "redis:7.4"

// client is shared by all tests in the package; each test should use its own
// key prefix. It is nil if the container couldn't be started.
var (
	client     *redis.Client
	startupErr error
)

// Run starts a Redis container, runs the tests and removes the container.
// Call it from TestMain: os.Exit(redistest.Run(m)).
// If the container can't be started, the tests still run and Client skips them.
func Run(m *testing.M) int {
	ctx := context.Background()
	container, err := tcredis.Run(ctx, Image)
	if err != nil {
		startupErr = err
		return m.Run()
	}
	defer func() {
		if err := container.Terminate(ctx); err != nil {
			log.Printf("terminate redis container: %v", err)
		}
	}()
	uri, err := container.ConnectionString(ctx)
	if err != nil {
		startupErr = err
		return m.Run()
	}
	opts, err := redis.ParseURL(uri)
	if err != nil {
		startupErr = err
		return m.Run()
	}
	client = redis.NewClient(opts)
	defer client.Close()
	return m.Run()
}

// prefixCounter makes prefixes unique when a test runs repeatedly (go test -count).
var prefixCounter atomic.Int64

// Prefix returns a key prefix unique to this test run, for use with store.NewRedis,
// and deletes the keys under it when the test ends. It skips the test if Redis
// isn't available.
func Prefix(t testing.TB) string {
	t.Helper()
	c := Client(t)
	prefix := fmt.Sprintf("test:%s:%d", safeName(t.Name()), prefixCounter.Add(1))
	t.Cleanup(func() {
		ctx := context.Background()
		// Keys are written as "{prefix}:name" by store.NewRedis.
		iter := c.Scan(ctx, 0, "{"+prefix+"}:*", 100).Iterator()
		for iter.Next(ctx) {
			c.Del(ctx, iter.Val())
		}
		if err := iter.Err(); err != nil {
			t.Logf("delete test keys: %v", err)
		}
	})
	return prefix
}

// safeName replaces every character of a test name except letters, digits,
// _, - and / with _, so that the prefix has no characters with a special
// meaning in SCAN patterns (* ? [ ] \) or hash tags ({ }). The counter in the
// prefix keeps names that end up equal apart.
func safeName(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-', r == '/':
			return r
		default:
			return '_'
		}
	}, name)
}

// Client returns the shared client, or skips the test if Redis isn't available.
func Client(t testing.TB) *redis.Client {
	t.Helper()
	if client == nil {
		t.Skipf("Redis container not available (is Docker running?): %v", startupErr)
	}
	return client
}
