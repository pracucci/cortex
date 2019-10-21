package main

import "math/rand"

type balancer interface {
	nextTargetID() int
}

type roundRobinBalancer struct {
	targets int
	next    int
}

func newRoundRobinBalancer(targets int) *roundRobinBalancer {
	return &roundRobinBalancer{
		targets: targets,
	}
}

func (b *roundRobinBalancer) nextTargetID() int {
	if b.next < b.targets-1 {
		b.next++
	} else {
		b.next = 0
	}

	return b.next
}

type randomBalancer struct {
	targets int
	next    int
}

func newRandomBalancer(targets int) *randomBalancer {
	return &randomBalancer{
		targets: targets,
	}
}

func (b *randomBalancer) nextTargetID() int {
	b.next = rand.Intn(b.targets)

	return b.next
}
