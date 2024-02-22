package kv

import "time"

type Param struct {
	ttl time.Duration
}

func Lifetime(v time.Duration) func(p *Param) {
	return func(p *Param) {
		p.ttl = v
	}
}
