package postgres_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/store/postgres"
)

// --- NewProjectionPersistence constructor validation tests ---
// These tests do not require a running database; they exercise the compile-time
// struct introspection performed by NewProjectionPersistence.

func TestNewProjectionPersistence_NonStructTypeRejected(t *testing.T) {
	_, err := postgres.NewProjectionPersistence[string](nil, "table")
	if err == nil {
		t.Error("expected error when V is not a struct")
	}
}

func TestNewProjectionPersistence_IntTypeRejected(t *testing.T) {
	_, err := postgres.NewProjectionPersistence[int](nil, "table")
	if err == nil {
		t.Error("expected error when V is int")
	}
}

func TestNewProjectionPersistence_StructWithNoTaggedFieldsRejected(t *testing.T) {
	type noTags struct {
		Foo string
		Bar int
	}
	_, err := postgres.NewProjectionPersistence[noTags](nil, "table")
	if err == nil {
		t.Error("expected error for struct with no proj-tagged fields")
	}
}

func TestNewProjectionPersistence_UnexportedTaggedFieldRejected(t *testing.T) {
	type withUnexported struct {
		name string `proj:"name"` //nolint:unused
	}
	_, err := postgres.NewProjectionPersistence[withUnexported](nil, "table")
	if err == nil {
		t.Error("expected error when proj-tagged field is unexported")
	}
}

func TestNewProjectionPersistence_ValidStructSucceeds(t *testing.T) {
	type myRow struct {
		Name    string `proj:"name"`
		Balance int64  `proj:"balance"`
	}
	p, err := postgres.NewProjectionPersistence[myRow](nil, "test_table")
	if err != nil {
		t.Fatalf("unexpected error for valid struct: %v", err)
	}
	if p == nil {
		t.Error("expected non-nil persistence")
	}
}

func TestNewProjectionPersistence_IgnoresDashTag(t *testing.T) {
	type withDash struct {
		Keep   string `proj:"keep"`
		Ignore string `proj:"-"`
	}
	// Should succeed; the "-" field is ignored.
	_, err := postgres.NewProjectionPersistence[withDash](nil, "table")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewProjectionPersistence_IgnoresUntaggedExportedFields(t *testing.T) {
	type mixed struct {
		Name    string `proj:"name"`
		Ignored int    // no tag
	}
	_, err := postgres.NewProjectionPersistence[mixed](nil, "table")
	if err != nil {
		t.Fatalf("unexpected error for struct with untagged fields: %v", err)
	}
}

func TestNewProjectionPersistence_DuplicateColumnTagRejected(t *testing.T) {
	type withDupe struct {
		A string `proj:"name"`
		B string `proj:"name"`
	}
	_, err := postgres.NewProjectionPersistence[withDupe](nil, "table")
	if err == nil {
		t.Error("expected error for duplicate proj tag value")
	}
}

func TestNewProjectionPersistence_AllSupportedFieldTypes(t *testing.T) {
	type allTypes struct {
		S    string          `proj:"s"`
		I    int64           `proj:"i"`
		F64  float64         `proj:"f64"`
		F32  float32         `proj:"f32"`
		B    bool            `proj:"b"`
		T    time.Time       `proj:"t"`
		Raw  []byte          `proj:"raw"`
		JSON json.RawMessage `proj:"json"`
	}
	_, err := postgres.NewProjectionPersistence[allTypes](nil, "table")
	if err != nil {
		t.Fatalf("unexpected error for all-supported-types struct: %v", err)
	}
}

func TestNewProjectionPersistence_UnsupportedTypeRejected(t *testing.T) {
	type withSlice struct {
		Name string   `proj:"name"`
		Tags []string `proj:"tags"`
	}
	_, err := postgres.NewProjectionPersistence[withSlice](nil, "table")
	if err == nil {
		t.Error("expected error for unsupported []string field type")
	}
}

func TestNewProjectionPersistence_StructFieldRejected(t *testing.T) {
	type withStruct struct {
		Name  string     `proj:"name"`
		Inner struct{ X int } `proj:"inner"`
	}
	_, err := postgres.NewProjectionPersistence[withStruct](nil, "table")
	if err == nil {
		t.Error("expected error for unsupported struct field type")
	}
}
