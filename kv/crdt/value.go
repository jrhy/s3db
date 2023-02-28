package crdt

type Value struct {
	ModEpochNanos            int64       `json:"m"`
	PreviousRoot             string      `json:"p,omitempty"`
	TombstoneSinceEpochNanos int64       `json:"d,omitempty"`
	Value                    interface{} `json:"v"`
}

func (v Value) Tombstoned() bool {
	return v.TombstoneSinceEpochNanos != 0
}

func LastWriteWins(newValue, oldValue *Value) *Value {
	if newValue.Tombstoned() || oldValue.Tombstoned() {
		return firstTombstoneWins(newValue, oldValue)
	}
	if newValue.ModEpochNanos >= oldValue.ModEpochNanos {
		return newValue
	}
	return oldValue
}

func firstTombstoneWins(newValue, oldValue *Value) *Value {
	if !newValue.Tombstoned() {
		return oldValue
	}
	if !oldValue.Tombstoned() {
		return newValue
	}
	if newValue.TombstoneSinceEpochNanos < oldValue.TombstoneSinceEpochNanos {
		return newValue
	}
	return oldValue
}
