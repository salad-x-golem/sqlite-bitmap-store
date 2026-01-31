package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	arkivevents "github.com/salad-x-golem/arkiv-events"
	"github.com/salad-x-golem/arkiv-events/tariterator"
	sqlitestore "github.com/salad-x-golem/sqlite-bitmap-store"
	"github.com/urfave/cli/v2"
)

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := struct {
		dbPath string
	}{}

	app := &cli.App{
		Name:  "load-from-tar",
		Usage: "Load data from a node into a SQLite database",
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:        "db-path",
				Value:       "arkiv-data.db",
				Destination: &cfg.dbPath,
				EnvVars:     []string{"DB_PATH"},
			},
		},
		Action: func(c *cli.Context) error {

			tarFileName := c.Args().First()

			if tarFileName == "" {
				return fmt.Errorf("tar file is required")
			}

			tarFile, err := os.Open(tarFileName)
			if err != nil {
				return fmt.Errorf("failed to open tar file: %w", err)
			}
			defer tarFile.Close()

			store, err := sqlitestore.NewSQLiteStore(logger, cfg.dbPath, 7)
			if err != nil {
				return fmt.Errorf("failed to create SQLite store: %w", err)
			}
			defer store.Close()

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			iterator := tariterator.IterateTar(200, tarFile)

			err = store.FollowEvents(ctx, arkivevents.BatchIterator(iterator))
			if err != nil {
				return fmt.Errorf("failed to follow events: %w", err)
			}

			return nil

		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
