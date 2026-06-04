package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	datastar "github.com/starfederation/datastar-go/datastar"

	"github.com/jackc/pgx/v5/pgxpool"
)

const pageSize = 50

type server struct {
	db   *db
	tmpl *template.Template
}

func newServer(pool *pgxpool.Pool) *server {
	s := &server{db: &db{pool: pool}}
	s.tmpl = template.Must(template.New("").Funcs(template.FuncMap{
		"fmtTime":   fmtTime,
		"fmtPretty": fmtPretty,
		"fmtNum":    fmtNum,
		"lagClass":  lagClass,
		"rowVal":    func(row projectionRow, col string) string { return row.Values[col] },
	}).ParseFS(tmplFS, "tmpl/*.html"))
	return s
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleEvents)
	mux.HandleFunc("GET /events/rows", s.handleEventsRows)
	mux.HandleFunc("GET /events/tail", s.handleEventsTail)
	mux.HandleFunc("GET /aggregates", s.handleAggregates)
	mux.HandleFunc("GET /aggregates/{type}", s.handleAggregateType)
	mux.HandleFunc("GET /aggregates/{type}/{id}", s.handleAggregateDetail)
	mux.HandleFunc("GET /projections", s.handleProjections)
	mux.HandleFunc("GET /projections/{table}", s.handleProjectionDetail)
	mux.ServeHTTP(w, r)
}

// ── Events ──────────────────────────────────────────────────────────────────

type eventsData struct {
	Page      string
	Events    []eventRow
	MaxGC     int64
	OlderGC   int64 // non-zero if there are more events
	AggType   string
	AggID     string
	EventType string
	BeforeGC  int64
	Tail      bool
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	aggType := q.Get("agg_type")
	aggID := q.Get("agg_id")
	eventType := q.Get("event_type")
	beforeGC, _ := strconv.ParseInt(q.Get("before"), 10, 64)
	tail := q.Get("tail") == "1"

	ctx := r.Context()
	events, err := s.db.listEvents(ctx, aggType, aggID, eventType, beforeGC, pageSize+1)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	maxGC, _ := s.db.globalMaxCounter(ctx)

	var olderGC int64
	if len(events) > pageSize {
		events = events[:pageSize]
		olderGC = events[len(events)-1].GlobalCounter
	}

	s.render(w, "events.html", eventsData{
		Page:      "events",
		Events:    events,
		MaxGC:     maxGC,
		OlderGC:   olderGC,
		AggType:   aggType,
		AggID:     aggID,
		EventType: eventType,
		BeforeGC:  beforeGC,
		Tail:      tail,
	})
}

// handleEventsRows is the Data Star SSE endpoint for in-page filter updates.
func (s *server) handleEventsRows(w http.ResponseWriter, r *http.Request) {
	var sig struct {
		AggType   string `json:"aggType"`
		AggID     string `json:"aggID"`
		EventType string `json:"eventType"`
	}
	_ = datastar.ReadSignals(r, &sig)

	ctx := r.Context()
	events, err := s.db.listEvents(ctx, sig.AggType, sig.AggID, sig.EventType, 0, pageSize+1)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	var olderGC int64
	if len(events) > pageSize {
		events = events[:pageSize]
		olderGC = events[len(events)-1].GlobalCounter
	}

	html := s.fragment("events_tbody.html", map[string]any{
		"Events":  events,
		"OlderGC": olderGC,
		"AggType": sig.AggType,
		"AggID":   sig.AggID,
		"EventType": sig.EventType,
	})

	sse := datastar.NewSSE(w, r)
	_ = sse.PatchElements(html, datastar.WithSelectorID("events-tbody"), datastar.WithModeOuter())
}

// handleEventsTail is a long-running SSE endpoint that streams new events.
func (s *server) handleEventsTail(w http.ResponseWriter, r *http.Request) {
	var sig struct {
		TailGC int64 `json:"tailGC"`
	}
	_ = datastar.ReadSignals(r, &sig)
	afterGC := sig.TailGC

	sse := datastar.NewSSE(w, r)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			events, err := s.db.listEventsAfter(r.Context(), afterGC)
			if err != nil || len(events) == 0 {
				continue
			}

			// Build rows newest-first (reverse slice so prepend gives right order).
			var buf strings.Builder
			for i := len(events) - 1; i >= 0; i-- {
				buf.WriteString(s.fragment("event_row.html", events[i]))
			}
			afterGC = events[len(events)-1].GlobalCounter

			if err := sse.PatchElements(buf.String(),
				datastar.WithSelectorID("events-tbody"),
				datastar.WithModePrepend(),
			); err != nil {
				return
			}
			_ = sse.MarshalAndPatchSignals(map[string]any{"tailGC": afterGC})
		}
	}
}

// ── Aggregates ───────────────────────────────────────────────────────────────

func (s *server) handleAggregates(w http.ResponseWriter, r *http.Request) {
	types, err := s.db.listAggregateTypes(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.render(w, "aggregates.html", map[string]any{
		"Page":  "aggregates",
		"Types": types,
	})
}

func (s *server) handleAggregateType(w http.ResponseWriter, r *http.Request) {
	aggType := r.PathValue("type")
	ids, err := s.db.listAggregateIDs(r.Context(), aggType)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.render(w, "aggregate_type.html", map[string]any{
		"Page":          "aggregates",
		"AggregateType": aggType,
		"IDs":           ids,
	})
}

func (s *server) handleAggregateDetail(w http.ResponseWriter, r *http.Request) {
	aggType := r.PathValue("type")
	aggID := r.PathValue("id")
	events, err := s.db.listAggregateEvents(r.Context(), aggType, aggID)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.render(w, "aggregate_detail.html", map[string]any{
		"Page":          "aggregates",
		"AggregateType": aggType,
		"AggregateID":   aggID,
		"Events":        events,
	})
}

// ── Projections ──────────────────────────────────────────────────────────────

func (s *server) handleProjections(w http.ResponseWriter, r *http.Request) {
	projs, err := s.db.discoverProjections(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.render(w, "projections.html", map[string]any{
		"Page":        "projections",
		"Projections": projs,
	})
}

func (s *server) handleProjectionDetail(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	ctx := r.Context()

	// Verify this table is actually a projection (not arbitrary table access).
	projs, err := s.db.discoverProjections(ctx)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	var info *projectionInfo
	for i := range projs {
		if projs[i].TableName == tableName {
			info = &projs[i]
			break
		}
	}
	if info == nil {
		http.NotFound(w, r)
		return
	}

	cols, err := s.db.getProjectionColumns(ctx, tableName)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	rows, err := s.db.listProjectionRows(ctx, tableName, cols)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	s.render(w, "projection_detail.html", map[string]any{
		"Page":      "projections",
		"Info":      info,
		"TableName": tableName,
		"Columns":   cols,
		"Rows":      rows,
	})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func (s *server) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

func (s *server) fragment(name string, data any) string {
	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		return fmt.Sprintf("<!-- template error: %v -->", err)
	}
	return buf.String()
}

// ── Template functions ────────────────────────────────────────────────────────

func fmtTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}

func fmtPretty(raw json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		return string(raw)
	}
	return buf.String()
}

func fmtNum(n int64) string {
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	var out []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	return string(out)
}

func lagClass(lag int64) string {
	switch {
	case lag == 0:
		return "lag-ok"
	case lag < 500:
		return "lag-warn"
	default:
		return "lag-high"
	}
}

