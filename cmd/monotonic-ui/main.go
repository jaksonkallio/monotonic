package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dsn := flag.String("dsn", os.Getenv("DATABASE_URL"), "PostgreSQL connection string")
	addr := flag.String("addr", ":7654", "HTTP listen address")
	flag.Parse()

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "error: --dsn or DATABASE_URL required")
		os.Exit(1)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("ping: %v", err)
	}
	log.Printf("connected to postgres")

	s := newServer(pool)
	log.Printf("monotonic-ui listening on http://localhost%s", *addr)
	log.Fatal(http.ListenAndServe(*addr, s))
}
