// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import

// Token is used as a permission to keep on running.
"github.com/pingcap/tidb/trace_util_0"

type Token struct {
}

// TokenLimiter is used to limit the number of concurrent tasks.
type TokenLimiter struct {
	count uint
	ch    chan *Token
}

// Put releases the token.
func (tl *TokenLimiter) Put(tk *Token) {
	trace_util_0.Count(_tokenlimiter_00000, 0)
	tl.ch <- tk
}

// Get obtains a token.
func (tl *TokenLimiter) Get() *Token {
	trace_util_0.Count(_tokenlimiter_00000, 1)
	return <-tl.ch
}

// NewTokenLimiter creates a TokenLimiter with count tokens.
func NewTokenLimiter(count uint) *TokenLimiter {
	trace_util_0.Count(_tokenlimiter_00000, 2)
	tl := &TokenLimiter{count: count, ch: make(chan *Token, count)}
	for i := uint(0); i < count; i++ {
		trace_util_0.Count(_tokenlimiter_00000, 4)
		tl.ch <- &Token{}
	}

	trace_util_0.Count(_tokenlimiter_00000, 3)
	return tl
}

var _tokenlimiter_00000 = "server/tokenlimiter.go"
