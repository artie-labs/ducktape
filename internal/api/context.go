package api

import (
	"context"
	"database/sql"
)

type ctxKey struct{}

func WithDB(ctx context.Context, db *sql.DB) context.Context {
	return context.WithValue(ctx, ctxKey{}, db)
}

func DBFromContext(ctx context.Context) *sql.DB {
	db, _ := ctx.Value(ctxKey{}).(*sql.DB)
	return db
}
