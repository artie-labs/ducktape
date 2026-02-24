package utils

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
)

type Connector struct {
	connector *duckdb.Connector
	db        *sql.DB
}

func NewConnector(dsn string) (*Connector, error) {
	params := parseDSN(dsn)
	tokens, hasToken := params["motherduck_token"]
	if !hasToken {
		db, err := sql.Open("duckdb", dsn)
		if err != nil {
			return nil, fmt.Errorf("failed to open duckdb: %w", err)
		}
		return &Connector{db: db}, nil
	} else {
		params.Del("motherduck_token")
	}

	encodedParams := params.Encode()
	var inMemoryDSN string
	if encodedParams != "" {
		inMemoryDSN = fmt.Sprintf(":memory:?%s", encodedParams)
	} else {
		inMemoryDSN = ":memory:"
	}

	c, err := duckdb.NewConnector(inMemoryDSN, func(execer driver.ExecerContext) error {
		bootQueries := []string{
			`INSTALL motherduck`,
			`LOAD motherduck`,
			fmt.Sprintf("SET motherduck_token='%s'", escapeSQLString(tokens[0])),
			`ATTACH 'md:'`,
		}
		for _, query := range bootQueries {
			_, err := execer.ExecContext(context.Background(), query, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create connector: %w", err)
	}
	return &Connector{connector: c, db: sql.OpenDB(c)}, nil
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func parseDSN(dsn string) url.Values {
	empty := url.Values{}
	parts := strings.SplitN(dsn, "?", 2)
	if len(parts) != 2 {
		return empty
	}

	params, err := url.ParseQuery(parts[1])
	if err != nil {
		return empty
	}
	return params
}

func (c *Connector) GetDB() *sql.DB {
	return c.db
}

func (c *Connector) Close() error {
	var err1, err2 error
	if c.db != nil {
		err := c.db.Close()
		if err != nil {
			err1 = fmt.Errorf("failed to close database: %w", err)
		}
	}
	if c.connector != nil {
		err := c.connector.Close()
		if err != nil {
			err2 = fmt.Errorf("failed to close connector: %w", err)
		}
	}
	return errors.Join(err1, err2)
}
