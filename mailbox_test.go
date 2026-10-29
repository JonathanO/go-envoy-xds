package xds

import (
	"slices"
	"testing"
)

type summed []int

func (s summed) Merge(next summed) summed { return append(slices.Clone(s), next...) }

type phased struct {
	summed
	ends bool
}

func (p phased) Merge(next phased) phased { return phased{p.summed.Merge(next.summed), next.ends} }
func (p phased) MergesWithLater() bool    { return !p.ends }

func drain[U any](m *mailbox[U]) []U {
	var got []U
	for u, ok := m.take(); ok; u, ok = m.take() {
		got = append(got, u)
	}
	return got
}

func TestMailboxMergesWhatWaits(t *testing.T) {
	m := newMailbox[summed]()
	m.put(summed{1})
	m.put(summed{2})
	if got := drain(m); len(got) != 1 || !slices.Equal(got[0], summed{1, 2}) {
		t.Fatalf("got %v", got)
	}
	select {
	case <-m.ready:
	default:
		t.Fatal("not ready after a put")
	}
}

func TestMailboxQueuesWhatDoesNotMerge(t *testing.T) {
	m := newMailbox[int]()
	m.put(1)
	m.put(2)
	if got := drain(m); !slices.Equal(got, []int{1, 2}) {
		t.Fatalf("got %v", got)
	}
}

func TestMailboxMergesNothingIntoTheEndOfAPhase(t *testing.T) {
	m := newMailbox[phased]()
	m.put(phased{summed{1}, true})
	m.put(phased{summed{2}, false})
	m.put(phased{summed{3}, false})
	got := drain(m)
	if len(got) != 2 || !slices.Equal(got[0].summed, summed{1}) || !slices.Equal(got[1].summed, summed{2, 3}) {
		t.Fatalf("got %v", got)
	}
}

func TestClosedMailboxDropsEverything(t *testing.T) {
	m := newMailbox[int]()
	m.put(1)
	m.close()
	m.put(2)
	if got := drain(m); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}
