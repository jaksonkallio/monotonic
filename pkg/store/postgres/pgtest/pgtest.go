// Package pgtest provides a Postgres-backed integration test harness for projects
// that build on monotonic's Postgres store.
//
// It is shipped as its own Go module so that taking a dependency on the Postgres
// store does not pull in testcontainers and its large transitive dependency tree.
// Import this package only from _test.go files.
package pgtest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pgstore "github.com/jaksonkallio/monotonic/pkg/store/postgres"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ErrDockerUnavailable is returned (wrapped) when the test harness cannot reach a
// container runtime. Callers normally do not need to inspect this directly — Harness
// translates it into t.Skip so `go test ./...` stays green on machines without Docker.
var ErrDockerUnavailable = errors.New("docker unavailable")

// Components bundles everything Harness produces. The pool, store, and backend share
// the same underlying Postgres instance; the schema (events + projector_state) has
// already been migrated.
//
// Implementers are responsible for any application-specific schema migrations on top.
type Components struct {
	Pool    *pgxpool.Pool
	Store   *pgstore.Store
	Backend *pgstore.ProjectorBackend
}

var (
	bootOnce sync.Once
	booted   Components
	bootErr  error
)

// Harness lazily boots a Postgres container shared across the test binary, applies
// the event-store and projector-state migrations, and returns ready-to-use components.
// The first call boots the container; subsequent calls reuse it.
//
// If Docker is not reachable on the host, Harness calls t.Skip rather than failing,
// so test suites stay green on machines without a container runtime.
//
// Tests that need a clean event log should TRUNCATE events; tests that need a fresh
// projector_state row should DELETE FROM projector_state WHERE projector_name = ...
// or use unique projector names. The harness intentionally does not truncate
// automatically — it does not know which tables (if any) the implementer owns above
// the event store, and per-test isolation strategy is best left to the caller.
func Harness(t testing.TB) Components {
	t.Helper()
	bootOnce.Do(func() {
		booted, bootErr = boot()
	})
	if bootErr != nil {
		if errors.Is(bootErr, ErrDockerUnavailable) {
			t.Skipf("pgtest: docker not available (%v)", bootErr)
		}
		t.Fatalf("pgtest: boot harness: %v", bootErr)
	}
	return booted
}

func boot() (Components, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("monotonic_pgtest"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			// initdb starts Postgres twice during bootstrap, so the "ready" log line
			// appears twice. Waiting for the second occurrence avoids racing the
			// shutdown that happens between them.
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(45*time.Second),
		),
	)
	if err != nil {
		return Components{}, classifyBootError(err)
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return Components{}, err
	}
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return Components{}, err
	}

	store := pgstore.New(pool)
	if err := store.Migrate(ctx); err != nil {
		return Components{}, err
	}
	backend := pgstore.NewProjectorBackend(pool)
	if err := backend.Migrate(ctx); err != nil {
		return Components{}, err
	}
	return Components{Pool: pool, Store: store, Backend: backend}, nil
}

// classifyBootError wraps the docker-unavailable sentinel for common boot failures
// caused by a missing or unreachable container runtime, so tests skip instead of fail.
func classifyBootError(err error) error {
	msg := err.Error()
	needles := []string{
		"Cannot connect to the Docker daemon",
		"docker: not found",
		"docker daemon",
		"no such host",
		"connection refused",
		"permission denied",
		"failed to find a Docker client",
		"rootless Docker not found",
	}
	for _, n := range needles {
		if strings.Contains(msg, n) {
			return errors.Join(ErrDockerUnavailable, err)
		}
	}
	return err
}
