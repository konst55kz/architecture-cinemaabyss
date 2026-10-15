package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port                  string
	MonolithURL           string
	MoviesServiceURL      string
	EventsServiceURL      string
	GradualMigration      bool
	MoviesMigrationPercent int
}

func loadConfig() Config {
	gradualMigration := strings.ToLower(os.Getenv("GRADUAL_MIGRATION")) == "true"
	migrationPercent := 0
	if p, err := strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT")); err == nil {
		migrationPercent = p
	}

	return Config{
		Port:                  getEnv("PORT", "8000"),
		MonolithURL:           getEnv("MONOLITH_URL", "http://localhost:8080"),
		MoviesServiceURL:      getEnv("MOVIES_SERVICE_URL", "http://localhost:8081"),
		EventsServiceURL:      getEnv("EVENTS_SERVICE_URL", "http://localhost:8082"),
		GradualMigration:      gradualMigration,
		MoviesMigrationPercent: migrationPercent,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func newReverseProxy(target string) *httputil.ReverseProxy {
	u, err := url.Parse(target)
	if err != nil {
		log.Fatalf("invalid target URL %s: %v", target, err)
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("proxy error for %s %s: %v", r.Method, r.URL.Path, err)
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	}
	return proxy
}

func (cfg *Config) routeMovies(w http.ResponseWriter, r *http.Request, monolithProxy, moviesProxy *httputil.ReverseProxy) {
	if !cfg.GradualMigration {
		log.Printf("[proxy] MOVIES -> monolith (migration disabled): %s %s", r.Method, r.URL.Path)
		monolithProxy.ServeHTTP(w, r)
		return
	}

	if rand.Intn(100) < cfg.MoviesMigrationPercent {
		log.Printf("[proxy] MOVIES -> movies-service (%d%%): %s %s", cfg.MoviesMigrationPercent, r.Method, r.URL.Path)
		moviesProxy.ServeHTTP(w, r)
	} else {
		log.Printf("[proxy] MOVIES -> monolith (%d%% fallback): %s %s", 100-cfg.MoviesMigrationPercent, r.Method, r.URL.Path)
		monolithProxy.ServeHTTP(w, r)
	}
}

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	cfg := loadConfig()

	log.Printf("Starting proxy service on port %s", cfg.Port)
	log.Printf("Monolith:      %s", cfg.MonolithURL)
	log.Printf("Movies service: %s", cfg.MoviesServiceURL)
	log.Printf("Events service: %s", cfg.EventsServiceURL)
	log.Printf("Gradual migration: %v, movies migration percent: %d%%", cfg.GradualMigration, cfg.MoviesMigrationPercent)

	monolithProxy := newReverseProxy(cfg.MonolithURL)
	moviesProxy := newReverseProxy(cfg.MoviesServiceURL)
	eventsProxy := newReverseProxy(cfg.EventsServiceURL)

	mux := http.NewServeMux()

	// Health check for the proxy itself
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","service":"proxy","gradual_migration":%v,"movies_migration_percent":%d}`,
			cfg.GradualMigration, cfg.MoviesMigrationPercent)
	})

	// Events routes -> events-service
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[proxy] EVENTS -> events-service: %s %s", r.Method, r.URL.Path)
		eventsProxy.ServeHTTP(w, r)
	})
	mux.HandleFunc("/api/events/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[proxy] EVENTS -> events-service: %s %s", r.Method, r.URL.Path)
		eventsProxy.ServeHTTP(w, r)
	})

	// Movies routes -> gradual migration between monolith and movies-service
	mux.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		cfg.routeMovies(w, r, monolithProxy, moviesProxy)
	})
	mux.HandleFunc("/api/movies/", func(w http.ResponseWriter, r *http.Request) {
		cfg.routeMovies(w, r, monolithProxy, moviesProxy)
	})

	// All other routes -> monolith
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[proxy] DEFAULT -> monolith: %s %s", r.Method, r.URL.Path)
		monolithProxy.ServeHTTP(w, r)
	})

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	log.Fatal(server.ListenAndServe())
}
