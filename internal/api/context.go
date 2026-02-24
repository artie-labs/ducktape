package api

import (
	"context"
	"database/sql"
	"fmt"
)

type ctxKey struct{}

func WithDB(ctx context.Context, db *sql.DB) context.Context {
	return context.WithValue(ctx, ctxKey{}, db)
}

func DBFromContext(ctx context.Context) (*sql.DB, error) {
	db, ok := ctx.Value(ctxKey{}).(*sql.DB)
	if !ok || db == nil {
		return nil, fmt.Errorf("no database connection in context")
	}
	return db, nil
}
