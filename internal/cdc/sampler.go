package cdc

// KeySampler is told which rows the applier committed, so that verification has
// something to check that a physical sample of the source cannot reach.
//
// A sample of the source heap finds the rows the base copy wrote, because those
// are what fills a heap; the rows replication wrote are scattered wherever the
// free space map put them and are missed almost entirely. The applier is the only
// thing that knows which rows went down the replication path, so it is the only
// thing that can say.
//
// Observe is called from the apply loop and must not block it. An implementation
// that cannot keep up is expected to drop samples, which costs coverage; making
// apply wait for a sample to be recorded would cost replication lag.
type KeySampler interface {
	Observe(sample KeySample)
}

// KeySample is one applied change reduced to what identifies its row.
//
// The datums are the raw replication values. Rendering them to something a query
// can bind is deliberately left to the implementation, because it is the
// expensive part and it does not belong on the apply path.
type KeySample struct {
	Schema, Table string
	Kind          ChangeKind
	Columns       []SampleColumn
	LSN           LSN
}

// SampleColumn is one key column's name, type and value. The name is carried
// because the applier keys a change on the replica identity while verification
// keys a row on its primary key, and those are permitted to differ; matching by
// name is what lets the reader notice rather than compare the wrong columns.
type SampleColumn struct {
	Name  string
	OID   uint32
	Datum TupleDatum
}

// keySampleFor reduces a change to its replica identity. It returns false for a
// change with no usable identity, which is not an error: a relation published
// without a key, or a key column arriving as unchanged TOAST, simply cannot be
// named, and the applier's own predicate building rejects the latter too.
func keySampleFor(relation *Relation, change *Change, lsn LSN) (KeySample, bool) {
	if relation == nil || change == nil {
		return KeySample{}, false
	}
	tuple := change.New
	if change.Kind == ChangeDelete {
		tuple = change.Old
	}
	if tuple == nil {
		return KeySample{}, false
	}
	columns := make([]SampleColumn, 0, 2)
	for i := range relation.Columns {
		if relation.Columns[i].Flags&1 == 0 {
			continue
		}
		if i >= len(*tuple) {
			return KeySample{}, false
		}
		datum := (*tuple)[i]
		if datum.Kind == DatumUnchangedToast {
			return KeySample{}, false
		}
		// The datum's bytes are copied because a spilled transaction decodes its
		// changes from disk into a buffer it reuses, so holding the slice past
		// this call would hand the sampler whatever the next change decoded into.
		copied := TupleDatum{Kind: datum.Kind}
		if datum.Data != nil {
			copied.Data = append([]byte(nil), datum.Data...)
		}
		columns = append(columns, SampleColumn{
			Name:  relation.Columns[i].Name,
			OID:   relation.Columns[i].Type,
			Datum: copied,
		})
	}
	if len(columns) == 0 {
		return KeySample{}, false
	}
	return KeySample{
		Schema: relation.Namespace, Table: relation.Name,
		Kind: change.Kind, Columns: columns, LSN: lsn,
	}, true
}

// sampleCollector gathers a transaction's samples so they can be handed over
// after the commit rather than during it. A change recorded before the commit
// would name a row that a rolled back apply never wrote.
type sampleCollector struct {
	sampler  KeySampler
	relation map[uint32]*Relation
	samples  []KeySample
	lsn      LSN
}

func newSampleCollector(sampler KeySampler, transaction *Transaction) *sampleCollector {
	if sampler == nil || transaction == nil {
		return nil
	}
	relations := make(map[uint32]*Relation, len(transaction.Relations))
	for i := range transaction.Relations {
		relations[transaction.Relations[i].OID] = &transaction.Relations[i]
	}
	return &sampleCollector{
		sampler: sampler, relation: relations, lsn: transaction.EndLSN,
	}
}

func (c *sampleCollector) add(change *Change) {
	if c == nil || change == nil || change.Kind == ChangeTruncate {
		return
	}
	sample, ok := keySampleFor(c.relation[change.RelationOID], change, c.lsn)
	if !ok {
		return
	}
	c.samples = append(c.samples, sample)
}

func (c *sampleCollector) addAll(changes []Change) {
	if c == nil {
		return
	}
	for i := range changes {
		c.add(&changes[i])
	}
}

// flush hands the transaction's samples over. It is called only after the apply
// transaction has committed.
func (c *sampleCollector) flush() {
	if c == nil {
		return
	}
	for _, sample := range c.samples {
		c.sampler.Observe(sample)
	}
	c.samples = c.samples[:0]
}
