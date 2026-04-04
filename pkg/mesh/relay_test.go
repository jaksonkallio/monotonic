package mesh

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestSortPrioritizedRelays(t *testing.T) {
	// Helper to create a relay with just a priority (transport/timeout not needed for sorting)
	makeRelay := func(priority int) Relay {
		return Relay{Priority: priority}
	}

	t.Run("sorts by priority ascending", func(t *testing.T) {
		relays := []Relay{
			makeRelay(3),
			makeRelay(1),
			makeRelay(2),
		}

		sorted := SortPrioritizedRelays(rand.NewSource(42), relays)

		if len(sorted) != 3 {
			t.Fatalf("expected 3 relays, got %d", len(sorted))
		}
		if sorted[0].Priority != 1 || sorted[1].Priority != 2 || sorted[2].Priority != 3 {
			t.Errorf(
				"expected priorities [1, 2, 3], got [%d, %d, %d]",
				sorted[0].Priority,
				sorted[1].Priority,
				sorted[2].Priority,
			)
		}
	})

	t.Run("same seed produces same order for equal priorities", func(t *testing.T) {
		relays := []Relay{
			{Priority: 1, Name: "a"},
			{Priority: 1, Name: "b"},
			{Priority: 1, Name: "c"},
			{Priority: 1, Name: "d"},
		}

		const seed int64 = 12345

		sorted1 := SortPrioritizedRelays(rand.NewSource(seed), relays)
		sorted2 := SortPrioritizedRelays(rand.NewSource(seed), relays)

		for i := range sorted1 {
			if sorted1[i].Name != sorted2[i].Name {
				t.Errorf(
					"position %d: sorted1 has Name %q, sorted2 has Name %q",
					i,
					sorted1[i].Name,
					sorted2[i].Name,
				)
			}
		}
	})

	t.Run("different seeds produce different orders for equal priorities", func(t *testing.T) {
		relays := []Relay{
			{Priority: 1, Name: "a"},
			{Priority: 1, Name: "b"},
			{Priority: 1, Name: "c"},
			{Priority: 1, Name: "d"},
		}

		sorted1 := SortPrioritizedRelays(rand.NewSource(111), relays)
		sorted2 := SortPrioritizedRelays(rand.NewSource(222), relays)

		anyDifferent := false
		for i := range sorted1 {
			if sorted1[i].Name != sorted2[i].Name {
				anyDifferent = true
				break
			}
		}

		if !anyDifferent {
			t.Error("expected different seeds to produce different orderings, but they were identical")
		}
	})

	t.Run("mixed priorities with same-priority shuffling", func(t *testing.T) {
		relays := []Relay{
			{Priority: 2, Name: "p2-first"},
			{Priority: 1, Name: "p1-first"},
			{Priority: 1, Name: "p1-second"},
			{Priority: 3, Name: "p3-only"},
			{Priority: 2, Name: "p2-second"},
		}

		sorted := SortPrioritizedRelays(rand.NewSource(999), relays)

		if sorted[0].Priority != 1 || sorted[1].Priority != 1 {
			t.Errorf("expected first two relays to have priority 1")
		}
		if sorted[2].Priority != 2 || sorted[3].Priority != 2 {
			t.Errorf("expected middle two relays to have priority 2")
		}
		if sorted[4].Priority != 3 {
			t.Errorf("expected last relay to have priority 3")
		}
	})

	t.Run("empty slice", func(t *testing.T) {
		sorted := SortPrioritizedRelays(rand.NewSource(42), []Relay{})

		if len(sorted) != 0 {
			t.Errorf("expected empty slice, got %d elements", len(sorted))
		}
	})

	t.Run("single element", func(t *testing.T) {
		relays := []Relay{makeRelay(5)}

		sorted := SortPrioritizedRelays(rand.NewSource(42), relays)

		if len(sorted) != 1 || sorted[0].Priority != 5 {
			t.Errorf("expected single relay with priority 5, got %+v", sorted)
		}
	})

	t.Run("does not modify original slice", func(t *testing.T) {
		relays := []Relay{
			makeRelay(3),
			makeRelay(1),
			makeRelay(2),
		}

		_ = SortPrioritizedRelays(rand.NewSource(42), relays)

		// Original should be unchanged
		if relays[0].Priority != 3 || relays[1].Priority != 1 || relays[2].Priority != 2 {
			t.Errorf(
				"original slice was modified: got priorities [%d, %d, %d]",
				relays[0].Priority,
				relays[1].Priority,
				relays[2].Priority,
			)
		}
	})
}

func BenchmarkSortPrioritizedRelays(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		relays := make([]Relay, size)
		for i := range relays {
			relays[i] = Relay{
				Name:     fmt.Sprintf("relay-%d", i),
				Priority: i % 5,
			}
		}

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; b.Loop(); i++ {
				SortPrioritizedRelays(rand.NewSource(int64(i)), relays)
			}
		})
	}
}
