package monotonic

import (
	"context"
	"log"
	"time"
)

// SagaDriver continuously steps sagas forward.
// It polls the store for sagas of a given type and calls Step() on each.
//
// Multiple drivers can run simultaneously without coordination - optimistic
// concurrency ensures only one driver succeeds per saga step. This enables:
//   - Single-process: Run as a background goroutine
//   - Multi-process: Run as a dedicated long-running process
type SagaDriver struct {
	store    Store
	sagaType string
	actions  ActionMap
	interval time.Duration
	logger   Logger
}

// Logger is a simple logging interface for the driver
type Logger interface {
	Printf(format string, v ...any)
}

// defaultLogger wraps the standard log package
type defaultLogger struct{}

func (defaultLogger) Printf(format string, v ...any) {
	log.Printf(format, v...)
}

// SagaDriverConfig configures a SagaDriver
type SagaDriverConfig struct {
	// Store is the event store (required)
	Store Store

	// SagaType is the aggregate type for sagas to drive (required)
	SagaType string

	// Actions is the action map for the saga type (required)
	Actions ActionMap

	// Interval is how often to poll for sagas (default: 1 second)
	Interval time.Duration

	// Logger for driver events (default: standard log package)
	Logger Logger
}

// NewSagaDriver creates a new saga driver with the given configuration.
func NewSagaDriver(cfg SagaDriverConfig) *SagaDriver {
	interval := cfg.Interval
	if interval == 0 {
		interval = 1 * time.Second
	}

	logger := cfg.Logger
	if logger == nil {
		logger = defaultLogger{}
	}

	return &SagaDriver{
		store:    cfg.Store,
		sagaType: cfg.SagaType,
		actions:  cfg.Actions,
		interval: interval,
		logger:   logger,
	}
}

// Run continuously steps all sagas until the context is cancelled.
// This is the main entry point for running the driver.
//
// For single-process deployments, run this in a goroutine:
//
//	go driver.Run(ctx)
//
// For dedicated saga processes, run this as the main loop:
//
//	if err := driver.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
func (d *SagaDriver) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	// Run immediately on start
	if err := d.StepAll(ctx); err != nil {
		d.logger.Printf("saga driver: step error: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.StepAll(ctx); err != nil {
				d.logger.Printf("saga driver: step error: %v", err)
				// Continue running - individual saga errors shouldn't stop the driver
			}
		}
	}
}

// StepAll steps all sagas of the configured type once.
// Returns the first error encountered, but attempts to step all sagas.
//
// Use this for manual control or testing. For continuous operation, use Run().
func (d *SagaDriver) StepAll(ctx context.Context) error {
	ids, err := d.store.ListAggregates(d.sagaType)
	if err != nil {
		return err
	}

	var firstErr error
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := d.stepSaga(ctx, id); err != nil {
			d.logger.Printf("saga driver: error stepping %s/%s: %v", d.sagaType, id, err)
			if firstErr == nil {
				firstErr = err
			}
			// Continue to other sagas
		}
	}

	return firstErr
}

func (d *SagaDriver) stepSaga(ctx context.Context, id string) error {
	saga, err := LoadSaga(d.store, d.sagaType, id, d.actions)
	if err != nil {
		return err
	}

	// If closed, ensure store knows (crash recovery) and skip
	if saga.Closed {
		return d.store.Close(d.sagaType, id)
	}

	// Skip if not ready (delayed)
	if !saga.IsReady() {
		return nil
	}

	// Skip if no action for current state
	if _, exists := d.actions[saga.State]; !exists {
		return nil
	}

	return saga.Step(ctx)
}

// StepSaga steps a specific saga by ID.
// Useful for triggering immediate processing of a specific saga.
func (d *SagaDriver) StepSaga(ctx context.Context, id string) error {
	return d.stepSaga(ctx, id)
}
