package verify

import (
	"sync"
	"time"
)

// Side names which database a read is against, so a long silence is attributable
// to one of them rather than to verification in general. Only the source is paced
// and measured: the target is reached by key, in a batch per window, which is not
// work anyone needs an estimate for.
type Side string

const sideSource Side = "source"

// Stage names what a check is doing.
type Stage string

const (
	StageSampling   Stage = "sampling"
	StageRechecking Stage = "rechecking"
	StageCDC        Stage = "checking cdc"
	StageDone       Stage = "done"
)

// Progress is one observation of a running check.
//
// The work is paced in pages because that is what the source read costs: a window
// reads a fixed interval of the heap whatever the rows in it turn out to be, so
// pages give an estimate that does not move as row density changes, which on a
// bloated table it does by two orders of magnitude between regions.
//
// Rows against Estimated is a different figure and the more important one, because
// it is the only place the reader is told that this was a sample: pages read will
// reach the pages planned, and both are a fraction of PagesTotal.
type Progress struct {
	Table      string        `json:"table"`
	Side       Side          `json:"side,omitempty"`
	Stage      Stage         `json:"stage"`
	Pages      int64         `json:"pages"`
	PagesTotal int64         `json:"pages_total"`
	Rows       int64         `json:"rows"`
	Estimated  int64         `json:"estimated_rows"`
	TargetRows int64         `json:"target_rows"`
	Rate       float64       `json:"rows_per_second"`
	ETA        time.Duration `json:"eta"`
	Coverage   float64       `json:"coverage"`
	Candidates int           `json:"candidate_rows,omitempty"`
	// CDCKeys is how many applier-recorded rows this table's CDC stratum checked.
	// It is reported separately from Rows and never added to it: the two count
	// different rows found in different ways.
	CDCKeys int64 `json:"cdc_keys,omitempty"`
	// CDCObserved is how many changes the applier saw for the table, which is
	// what makes CDCKeys interpretable as coverage rather than as a bare count.
	CDCObserved int64 `json:"cdc_observed,omitempty"`
	Unresolved  int   `json:"unresolved,omitempty"`
	Converged   bool  `json:"converged,omitempty"`
	Complete    bool  `json:"complete,omitempty"`
}

// ProgressSink receives progress observations. Implementations must be safe for
// concurrent use, because tables are checked in parallel.
type ProgressSink interface {
	Update(Progress)
}

// rate tracks throughput per side with an exponentially weighted mean.
//
// It stays keyed by side even though only the source is measured, because the
// alternative is a single unlabelled rate that silently means "the source" and
// would have to be untangled the moment anything else is timed.
type rate struct {
	mu      sync.Mutex
	perSide map[Side]observation
}

type observation struct {
	rows  float64
	pages float64
}

const rateSmoothing = 0.3

func (r *rate) observe(side Side, rows, pages int64, elapsed time.Duration) {
	if elapsed <= 0 || pages <= 0 {
		return
	}
	seconds := elapsed.Seconds()
	current := observation{rows: float64(rows) / seconds, pages: float64(pages) / seconds}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.perSide == nil {
		r.perSide = make(map[Side]observation)
	}
	previous, seen := r.perSide[side]
	if !seen {
		r.perSide[side] = current
		return
	}
	r.perSide[side] = observation{
		rows:  previous.rows + rateSmoothing*(current.rows-previous.rows),
		pages: previous.pages + rateSmoothing*(current.pages-previous.pages),
	}
}

// rows is the measured row throughput of one side, which is the figure an operator
// recognises.
func (r *rate) rows(side Side) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.perSide[side].rows
}

// eta estimates the time left from the pages still to read, returning zero when
// nothing has been measured yet rather than inventing a number.
func (r *rate) eta(side Side, pages int64) time.Duration {
	if pages <= 0 {
		return 0
	}
	r.mu.Lock()
	current := r.perSide[side].pages
	r.mu.Unlock()
	if current <= 0 {
		return 0
	}
	return time.Duration(float64(pages) / current * float64(time.Second))
}
