package kv

import (
	"github.com/vela-ssoc/vela-kit/hashmap"
	"github.com/vela-ssoc/vela-kit/kind"
	"github.com/vela-ssoc/vela-kit/lua"
	"github.com/vela-ssoc/vela-kit/strutil"
	"time"
)

func (b *Bucket) String() string                         { return strutil.B2S(b.Json()) }
func (b *Bucket) Type() lua.LValueType                   { return lua.LTObject }
func (b *Bucket) AssertFloat64() (float64, bool)         { return 0, false }
func (b *Bucket) AssertString() (string, bool)           { return "", false }
func (b *Bucket) AssertFunction() (*lua.LFunction, bool) { return nil, false }
func (b *Bucket) Peek() lua.LValue                       { return b }

func (b *Bucket) Json() []byte {
	enc := kind.NewJsonEncoder()
	enc.Tab("")
	enc.KV("type", "kv.bucket")
	enc.KV("name", b.Name)
	enc.KV("audit", b.Audit)
	enc.KV("ttl", b.TTL)
	enc.End("}")
	return enc.Json()
}

func (b *Bucket) incrL(L *lua.LState) int {
	key := L.CheckString(1)
	n := L.IsInt(2)
	if n == 0 {
		n = 1
	}

	L.Push(b.incr(key, n))
	return 1
}

func (b *Bucket) getL(L *lua.LState) int {
	key := L.CheckString(1)
	r := b.Get(key)
	L.Push(r)
	return 1
}

func (b *Bucket) setL(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.Get(2)

	L.Push(b.Set(key, val))
	return 1
}

func (b *Bucket) replyL(L *lua.LState) int {
	b.Reply = L.IsTrue(1)
	return 0
}

func (b *Bucket) auditL(L *lua.LState) int {
	b.Audit = L.IsTrue(1)
	return 0
}

func (b *Bucket) ttlL(L *lua.LState) int {
	n := L.CheckInt(1)
	if n < 0 {
		L.ArgError(1, "must be int")
		return 0
	}

	b.TTL = time.Second * time.Duration(n)
	return 0
}

func (b *Bucket) hmapL(L *lua.LState) int {
	key := L.CheckString(1)
	tab := hashmap.CheckHMap(L, 2)

	L.Push(b.HMap(key, tab))
	return 1
}

func (b *Bucket) listL(L *lua.LState) int {
	key := L.CheckString(1)
	item := L.Get(2)
	L.Push(b.List(key, item.String()))
	return 1
}

func (b *Bucket) releaseL(L *lua.LState) int {
	L.Push(lua.LBool(b.Release()))
	return 1
}

func (b *Bucket) deleteL(L *lua.LState) int {
	key := L.CheckString(1)
	L.Push(lua.LBool(b.Delete(key)))
	return 1
}

func (b *Bucket) Index(L *lua.LState, key string) lua.LValue {
	switch key {
	case "get":
		return lua.NewFunction(b.getL)
	case "set":
		return lua.NewFunction(b.setL)
	case "release":
		return lua.NewFunction(b.releaseL)
	case "del":
		return lua.NewFunction(b.deleteL)
	case "list":
		return lua.NewFunction(b.listL)
	case "hmap":
		return lua.NewFunction(b.hmapL)
	case "incr":
		return lua.NewFunction(b.incrL)
	case "reply":
		return lua.NewFunction(b.replyL)
	case "audit":
		return lua.NewFunction(b.auditL)
	case "ttl":
		return lua.NewFunction(b.ttlL)
	}
	return lua.LNil
}
