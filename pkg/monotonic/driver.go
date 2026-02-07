package monotonic

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// SagaDriver continuously polls and steps all sagas of a particular type forward
// Multiple drivers can run simultaneously without coordination
// Optimistic concurrency ensures only one driver succeeds per saga step
// Either run the saga driver in a dedicated process (medium/large scales) or as a part of a monolithic service process (small/medium scales)
type SagaDriver struct {
	store    SagaStore
	sagaType string
	actions  ActionMap
	interval time.Duration
}

// SagaDriverConfig configures a SagaDriver
type SagaDriverConfig struct {
	// Store is the saga store
	Store SagaStore

	// SagaType is the aggregate type for sagas to drive
	SagaType string

	// Actions is the action map for the saga type
	Actions ActionMap

	// Interval is how often to poll for sagas (default: 1 second)
	Interval time.Duration
}

// NewSagaDriver creates a new saga driver with the given configuration.
func NewSagaDriver(cfg SagaDriverConfig) *SagaDriver {
	interval := cfg.Interval
	if interval == 0 {
		interval = 1 * time.Second
	}

	return &SagaDriver{
		store:    cfg.Store,
		sagaType: cfg.SagaType,
		actions:  cfg.Actions,
		interval: interval,
	}
}

// Run continuously steps all sagas at configured interval until the context is cancelled
// This is the main entry point for running the driver
func (d *SagaDriver) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.StepAll(ctx); err != nil {
				slog.Error("saga driver step all", "error", err)
			}
		}
	}
}

// StepAll steps all sagas of the configured type once
// Normally called repeatedly in `Run`, but also useful for testing purposes
func (d *SagaDriver) StepAll(ctx context.Context) error {
	ids, err := d.store.ListActiveSagas(d.sagaType)
	if err != nil {
		return fmt.Errorf("list active sagas: %w", err)
	}

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := d.stepSaga(ctx, id); err != nil {
			slog.Error("failed to step saga", "sagaType", d.sagaType, "id", id, "error", err)
		} else {
			slog.Info("stepped saga", "sagaType", d.sagaType, "id", id)
		}
	}

	return nil
}

func (d *SagaDriver) stepSaga(ctx context.Context, id string) error {
	saga, err := LoadSaga(d.store, d.sagaType, id, d.actions)
	if err != nil {
		return err
	}

	// Handle crash recovery: if saga events show closed but store doesn't know,
	// sync the store. This handles the case where we crashed between appending
	// the saga-completed event and calling MarkSagaCompleted.
	if saga.Completed() {
		if err := d.store.MarkSagaCompleted(d.sagaType, id); err != nil {
			return fmt.Errorf("mark saga completed: %w", err)
		}
		return nil
	}

	return saga.Step(ctx)
}
