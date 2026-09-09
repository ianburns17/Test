package data

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// NewUUIDv7 generates a standard RFC 9562 UUID Version 7 (time-ordered)
func NewUUIDv7() (uuid.UUID, error) {
	var u uuid.UUID
	now := time.Now().UTC().UnixMilli()

	// High 48 bits: milliseconds since Unix epoch
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(now))
	copy(u[0:6], ts[2:8])

	// 10 random bytes for entropy
	if _, err := rand.Read(u[6:16]); err != nil {
		return uuid.Nil, fmt.Errorf("failed to generate random bytes for uuidv7: %w", err)
	}

	// Set version 7 (0b0111 in bits 4-7 of byte 6)
	u[6] = (u[6] & 0x0f) | 0x70

	// Set variant 1 (0b10 in bits 6-7 of byte 8)
	u[8] = (u[8] & 0x3f) | 0x80

	return u, nil
}

// NewUUIDv4 generates a standard random UUID Version 4
func NewUUIDv4() uuid.UUID {
	return uuid.New()
}
