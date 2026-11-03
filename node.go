package main

import (
	"log"
	"strconv"
	"sync"
)

type Clock struct {
	lamport int32
	mu      sync.Mutex
}

func (c *Clock) sendTick() int32 {
	c.mu.Lock()
	c.lamport++
	returnValue := c.lamport
	log.Println("[TICK] Lamport ticked to: " + strconv.Itoa(int(c.lamport)))
	c.mu.Unlock()

	return returnValue
}
