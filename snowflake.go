package snowflake

import (
	"errors"
	"sync"
	"time"
)

const (
	// epoch - customized time offset, milliseconds
	// Oct 11, 2019 20:18:46.794 UTC, incidentally equal to the first few digits of PI/2.
	epoch int64 = 1570796326794

	// number of bits allocated for the server id (max 1023).
	numWorkerBit = 10
	// number of bits allocated for the counter per millisecond.
	numSequenceBits = 12

	// workerId mask.
	maxWorkerID = -1 ^ (-1 << numWorkerBit)
	// sequence mask.
	maxSequence = -1 ^ (-1 << numSequenceBits)
)

// Snowflake is a structure which holds snowflak-specific data.
type Snowflake struct {
	lock          sync.Mutex
	lastTimestamp uint64
	sequence      uint32
	workerID      uint32
}

// NewSnowflake initialized the snowflake generator.
func NewSnowflake(workerID uint32) (*Snowflake, error) {
	if workerID > maxWorkerID {
		return nil, errors.New("invalid worker ID")
	}
	return &Snowflake{workerID: workerID}, nil
}

// Next generates the next sequence ID.
func (sf *Snowflake) Next() (uint64, error) {
	sf.lock.Lock()
	defer sf.lock.Unlock()

	ts := timestamp()
	if ts == sf.lastTimestamp {
		sf.sequence = (sf.sequence + 1) & maxSequence
		if sf.sequence == 0 {
			ts = sf.waitNextMillis(ts)
		}
	} else {
		sf.sequence = 0
	}

	if ts < sf.lastTimestamp {
		return 0, errors.New("invalid system clock")
	}
	sf.lastTimestamp = ts
	return sf.pack(), nil
}

// Sequence exhausted. Wait till the next millisecond.
func (sf *Snowflake) waitNextMillis(ts uint64) uint64 {
	for ts == sf.lastTimestamp {
		time.Sleep(100 * time.Microsecond)
		ts = timestamp()
	}
	return ts
}

// pack bits into a snowflake value.
func (sf *Snowflake) pack() uint64 {
	return (sf.lastTimestamp << (numWorkerBit + numSequenceBits)) |
		(uint64(sf.workerID) << numSequenceBits) |
		(uint64(sf.sequence))
}

// Convert from nanoseconds to milliseconds, adjust for the customized epoch.
func timestamp() uint64 {
	return uint64(time.Now().UnixNano()/int64(1000000) - epoch)
}
