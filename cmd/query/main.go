package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	sqlitestore "github.com/salad-x-golem/sqlite-bitmap-store"
	"github.com/urfave/cli/v2"
)

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := struct {
		dbPath string
	}{}

	app := &cli.App{
		Name:  "query",
		Usage: "Query the SQLite database",
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:        "db-path",
				Value:       "arkiv-data.db",
				Destination: &cfg.dbPath,
				EnvVars:     []string{"DB_PATH"},
			},
		},
		Action: func(c *cli.Context) error {

			queryString := c.Args().First()

			if queryString == "" {
				return fmt.Errorf("query is required")
			}

			st, err := sqlitestore.NewSQLiteStore(logger, cfg.dbPath, 7)
			if err != nil {
				return fmt.Errorf("failed to create SQLite store: %w", err)
			}
			defer st.Close()

			// q, err := query.Parse(queryString)
			// if err != nil {
			// 	return fmt.Errorf("failed to parse query: %w", err)
			// }

			startTime := time.Now()

			r, err := st.QueryEntities(
				context.Background(),
				queryString,
				// nil,
				&sqlitestore.Options{
					IncludeData: &sqlitestore.IncludeData{
						Key:                         true,
						ContentType:                 true,
						Payload:                     true,
						Attributes:                  true,
						SyntheticAttributes:         true,
						Expiration:                  true,
						Owner:                       true,
						CreatedAtBlock:              true,
						LastModifiedAtBlock:         true,
						TransactionIndexInBlock:     true,
						OperationIndexInTransaction: true,
					},
				},
			)

			duration := time.Since(startTime)

			if err != nil {
				return fmt.Errorf("failed to query entities: %w", err)
			}

			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			enc.Encode(r)

			fmt.Fprintf(os.Stderr, "Query time: %s\n", duration)

			return nil

		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
