package sqlite

import (
	"context"
	"errors"
	aa_sqlite "github.com/aaronland/go-sqlite"
	aa_database "github.com/aaronland/go-sqlite/database"
	"github.com/whosonfirst/go-cache"
	"io"
	"net/url"
)

type SQLiteCache struct {
	cache.Cache
	db    aa_sqlite.Database
	cache aa_sqlite.Table
}

func init() {

	ctx := context.Background()
	err := cache.RegisterCache(ctx, "sqlite", NewSQLiteCache)

	if err != nil {
		panic(err)
	}
}

func NewSQLiteCache(ctx context.Context, uri string) (cache.Cache, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	q := u.Query()
	dsn := q.Get("dsn")

	db, err := aa_database.NewDB(ctx, dsn)

	if err != nil {
		return nil, err
	}

	err = db.LiveHardDieFast()

	if err != nil {
		return nil, err
	}

	cache_tbl, err := NewCacheTableWithDatabase(ctx, db)

	if err != nil {
		return nil, err
	}

	c := &SQLiteCache{
		db:    db,
		cache: cache_tbl,
	}

	return c, nil
}

func (c *SQLiteCache) Name() string {
	return "sqlite"
}

func (c *SQLiteCache) Close(ctx context.Context) error {
	return errors.New("Not implemented")
}

func (c *SQLiteCache) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("Not implemented")
}

func (c *SQLiteCache) Set(context.Context, string, io.ReadCloser) (io.ReadCloser, error) {
	return nil, errors.New("Not implemented")
}

func (c *SQLiteCache) Unset(context.Context, string) error {
	return errors.New("Not implemented")
}

func (c *SQLiteCache) Hits() int64 {
	return -1
}

func (c *SQLiteCache) Misses() int64 {
	return -1
}

func (c *SQLiteCache) Evictions() int64 {
	return -1
}

func (c *SQLiteCache) Size() int64 {
	return -1
}

func (c *SQLiteCache) SizeWithContext(context.Context) int64 {
	return -1
}
