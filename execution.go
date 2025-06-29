package golite

// Filter is an execution primitive that takes a RecordIterator and a predicate function.
// It returns a new iterator that only yields rows for which the predicate returns true.
func Filter(input RecordIterator, predicate func(record Record) (bool, error)) RecordIterator {
	return func(yield func(Record, error) bool) {
		for record, err := range input {
			if err != nil {
				yield(nil, err)
				return // Stop on error
			}

			matches, err := predicate(record)
			if err != nil {
				yield(nil, err)
				return // Stop on error
			}

			if matches {
				if !yield(record, nil) {
					return // Stop if consumer requested it
				}
			}
		}
	}
}
