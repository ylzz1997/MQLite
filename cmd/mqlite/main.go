package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"mqlite/internal/broker"
	"mqlite/internal/config"
	"mqlite/internal/persistence"
	"mqlite/internal/server"
)

var (
	version   = "0.1.0"
	buildTime = "unknown"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to configuration file")
	showVersion := flag.Bool("version", false, "show version")
	flag.Parse()

	if *showVersion {
		fmt.Printf("MQLite %s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	logger := initLogger(cfg.Log)
	defer logger.Sync()

	logger.Info("MQLite starting",
		zap.String("version", version),
		zap.String("config", *configPath))

	// --- Persistence: AOF recovery ---
	var aofWriter *persistence.AOFWriter
	var rewriter *persistence.Rewriter
	var recoveryState *persistence.RecoveryState

	if cfg.Persistence.Enabled {
		// Recover state from AOF
		recoveryState, err = persistence.Recover(
			cfg.Persistence.AOF.Dir,
			cfg.Persistence.AOF.Filename,
			cfg.AckTimeout,
			logger,
		)
		if err != nil {
			logger.Fatal("AOF recovery failed", zap.Error(err))
		}

		// Create AOF writer
		aofWriter, err = persistence.NewAOFWriter(
			cfg.Persistence.AOF.Dir,
			cfg.Persistence.AOF.Filename,
			cfg.Persistence.AOF.Fsync,
			cfg.Persistence.AOF.RewriteMinSize,
			cfg.Persistence.AOF.RewritePercentage,
			logger,
		)
		if err != nil {
			logger.Fatal("failed to create AOF writer", zap.Error(err))
		}

		rewriter = persistence.NewRewriter(
			cfg.Persistence.AOF.Dir,
			cfg.Persistence.AOF.Filename,
			logger,
		)
	}

	// --- Create Broker ---
	b := broker.New(logger, aofWriter, rewriter, cfg.AckTimeout)

	// Load recovered state
	if recoveryState != nil && len(recoveryState.Namespaces) > 0 {
		b.LoadState(recoveryState)
	}

	// --- Start periodic AOF rewrite check ---
	if cfg.Persistence.Enabled {
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				b.TriggerRewrite()
			}
		}()
	}

	// --- Start servers ---
	if cfg.Server.GRPC.Enabled {
		grpcSrv := server.NewGRPCServer(b, cfg.Server.GRPC.Port, logger)
		if err := grpcSrv.Start(); err != nil {
			logger.Fatal("failed to start gRPC server", zap.Error(err))
		}
		defer grpcSrv.Stop()
		logger.Info("gRPC server ready", zap.Int("port", cfg.Server.GRPC.Port))
	}

	if cfg.Server.HTTP.Enabled {
		httpSrv := server.NewHTTPServer(b, cfg.Server.HTTP.Port, logger)
		if err := httpSrv.Start(); err != nil {
			logger.Fatal("failed to start HTTP server", zap.Error(err))
		}
		defer httpSrv.Stop()
		logger.Info("HTTP server ready", zap.Int("port", cfg.Server.HTTP.Port))
	}

	if cfg.Server.TCP.Enabled {
		tcpSrv := server.NewTCPServer(b, cfg.Server.TCP.Port, logger)
		if err := tcpSrv.Start(); err != nil {
			logger.Fatal("failed to start TCP server", zap.Error(err))
		}
		defer tcpSrv.Stop()
		logger.Info("TCP server ready", zap.Int("port", cfg.Server.TCP.Port))
	}

	logger.Info("MQLite is ready",
		zap.Bool("grpc", cfg.Server.GRPC.Enabled),
		zap.Bool("http", cfg.Server.HTTP.Enabled),
		zap.Bool("tcp", cfg.Server.TCP.Enabled),
		zap.Bool("persistence", cfg.Persistence.Enabled))

	// --- Wait for shutdown signal ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	logger.Info("shutdown signal received", zap.String("signal", sig.String()))
	logger.Info("shutting down broker...")

	if err := b.Close(); err != nil {
		logger.Error("broker close error", zap.Error(err))
	}

	logger.Info("MQLite stopped")
}

func initLogger(cfg config.LogConfig) *zap.Logger {
	var level zapcore.Level
	switch cfg.Level {
	case "debug":
		level = zapcore.DebugLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	default:
		level = zapcore.InfoLevel
	}

	var zapCfg zap.Config
	if cfg.Format == "json" {
		zapCfg = zap.NewProductionConfig()
	} else {
		zapCfg = zap.NewDevelopmentConfig()
	}
	zapCfg.Level = zap.NewAtomicLevelAt(level)
	zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := zapCfg.Build()
	if err != nil {
		panic(fmt.Sprintf("failed to init logger: %v", err))
	}

	return logger
}
