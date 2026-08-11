package verify

// hashSeed seeds every row hash. It is fixed rather than derived from anything, so
// two servers hash the same row to the same value and a result is comparable
// between runs.
const hashSeed int64 = 42

// renderedRow is the per-row text the hash is taken over. It renders the whole
// row, which is what makes the check sensitive to column values rather than only
// to which keys exist, and is why there is no separate existence check.
const renderedRow = "row(t.*)::text"
