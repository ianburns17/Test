package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"image-api/internal/data"
	_ "github.com/lib/pq"
)

type serverConfig struct {
	port        int
	environment string
	storageDir      string
	variantsDir     string
	processingDelay time.Duration
	db              struct {
		dsn          string
		maxOpenConns int
		maxIdleConns int
		maxIdleTime  time.Duration
	}
	limiter struct {
		rps     float64
		burst   int
		enabled bool
	}
}

type applicationDependencies struct {
	config   serverConfig
	logger   *slog.Logger
	models   data.Models
	notifier *JobNotifier
	wg       sync.WaitGroup
	dbMu     sync.Mutex
}

func main() {
	var cfg serverConfig

	flag.IntVar(&cfg.port, "port", 4000, "API server port")
	flag.StringVar(&cfg.environment, "env", "development", "Environment (development|staging|production)")
	flag.StringVar(&cfg.storageDir, "storage-dir", "./storage/uploads", "Directory to store accepted original images")
	flag.StringVar(&cfg.variantsDir, "variants-dir", "./storage/variants", "Directory to store generated image variants")

	// Read DSN from environment variable first, fallback to flag default
	defaultDSN := os.Getenv("IMAGE_DB_DSN")
	if defaultDSN == "" {
		defaultDSN = os.Getenv("GATEKEEPER_DB_DSN")
	}
	if defaultDSN == "" {
		defaultDSN = "postgres://postgres:postgres@localhost:5432/image_db?sslmode=disable"
	}

	flag.StringVar(&cfg.db.dsn, "db-dsn", defaultDSN, "PostgreSQL DSN")
	flag.StringVar(&cfg.db.dsn, "database", defaultDSN, "PostgreSQL DSN (alias for -db-dsn)")
	flag.IntVar(&cfg.db.maxOpenConns, "db-max-open-conns", 25, "PostgreSQL max open connections")
	flag.IntVar(&cfg.db.maxIdleConns, "db-max-idle-conns", 25, "PostgreSQL max idle connections")
	flag.DurationVar(&cfg.db.maxIdleTime, "db-max-idle-time", 15*time.Minute, "PostgreSQL max connection idle time")

	flag.DurationVar(&cfg.processingDelay, "processing-delay", 0, "Simulated delay for background worker job processing (e.g. 15s)")
	flag.DurationVar(&cfg.processingDelay, "delay", 0, "Simulated delay for background worker job processing (alias for -processing-delay)")

	flag.Float64Var(&cfg.limiter.rps, "limiter-rps", 50, "Rate limiter maximum requests per second")
	flag.IntVar(&cfg.limiter.burst, "limiter-burst", 100, "Rate limiter maximum burst")
	flag.BoolVar(&cfg.limiter.enabled, "limiter-enabled", true, "Enable rate limiter")

	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Ensure local storage directories exist
	if err := os.MkdirAll(cfg.storageDir, 0755); err != nil {
		logger.Error("failed to create image storage directory", "error", err, "dir", cfg.storageDir)
		os.Exit(1)
	}
	if err := os.MkdirAll(cfg.variantsDir, 0755); err != nil {
		logger.Error("failed to create variants storage directory", "error", err, "dir", cfg.variantsDir)
		os.Exit(1)
	}

	// Attempt to connect to PostgreSQL
	db, err := openDB(cfg)
	var models data.Models
	if err != nil {
		logger.Warn("could not connect to PostgreSQL; running in acceptance-only testing mode until DB is online", "error", err)
		models = data.NewModels(nil)
	} else {
		defer db.Close()
		logger.Info("database connection pool established", "dsn", cfg.db.dsn)
		models = data.NewModels(db)
	}

	app := &applicationDependencies{
		config:   cfg,
		logger:   logger,
		models:   models,
		notifier: NewJobNotifier(),
	}

	// Start exactly one background worker goroutine inside the application process (WRK-01)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()
	app.startWorker(workerCtx)

	apiServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.port),
		Handler:      app.routes(),
		IdleTimeout:  time.Minute,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		ErrorLog:     slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}

	// Graceful shutdown coordination:
	// 1. Listen for SIGINT / SIGTERM signals.
	// 2. Stop HTTP server from accepting new connections via apiServer.Shutdown().
	// 3. Stop the background worker via workerCancel().
	// 4. Wait for running background tasks via app.wg.Wait().
	shutdownError := make(chan error)
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		s := <-quit
		logger.Info("shutting down server", "signal", s.String())

		// Stop worker from picking up new jobs
		workerCancel()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		err := apiServer.Shutdown(ctx)
		if err != nil {
			shutdownError <- err
			return
		}

		logger.Info("waiting for background tasks to complete")
		app.wg.Wait()

		shutdownError <- nil
	}()

	logger.Info("starting image-api server",
		"address", apiServer.Addr,
		"environment", cfg.environment,
		"storage_dir", cfg.storageDir,
		"variants_dir", cfg.variantsDir,
		"processing_delay", cfg.processingDelay.String(),
	)

	err = apiServer.ListenAndServe()
	if !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server encountered fatal error", "error", err)
		os.Exit(1)
	}

	err = <-shutdownError
	if err != nil {
		logger.Error("shutdown error", "error", err)
		os.Exit(1)
	}
	logger.Info("server stopped gracefully")
}

func openDB(cfg serverConfig) (*sql.DB, error) {
	dsn := cfg.db.dsn
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/image_db?sslmode=disable"
	}
	if !strings.Contains(dsn, "sslmode=") {
		if strings.Contains(dsn, "?") {
			dsn += "&sslmode=disable"
		} else {
			dsn += "?sslmode=disable"
		}
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(cfg.db.maxOpenConns)
	db.SetMaxIdleConns(cfg.db.maxIdleConns)
	db.SetConnMaxIdleTime(cfg.db.maxIdleTime)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// tryConnectDB dynamically attempts to connect or reconnect to PostgreSQL if not currently connected.
func (a *applicationDependencies) tryConnectDB() bool {
	a.dbMu.Lock()
	defer a.dbMu.Unlock()

	if a.models.Jobs.DB != nil {
		return true
	}

	db, err := openDB(a.config)
	if err != nil {
		return false
	}

	a.models = data.NewModels(db)
	a.logger.Info("database connection pool dynamically established", "dsn", a.config.db.dsn)
	return true
}
