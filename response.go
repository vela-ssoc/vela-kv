package kv

import (
	strutil "github.com/vela-ssoc/vela-kit/auxlib"
	"github.com/vela-ssoc/vela-kit/lua"
	"github.com/vela-ssoc/vela-kit/reflectx"
	"time"
)

type Response struct {
	Err       error       `json:"-"`
	Bucket    string      `json:"bucket"`
	Key       string      `json:"key"`
	Value     *element    `json:"value"`
	LifeTime  int64       `json:"lifetime"`
	Count     int         `json:"count"`
	ExpiredAt time.Time   `json:"expired_at"`
	UpdatedAt time.Time   `json:"updated_at"`
	CreatedAt time.Time   `json:"created_at"`
	Element   interface{} `json:"-"`
}

func (r *Response) String() string       { return r.text() }
func (r *Response) Type() lua.LValueType { return lua.LTObject }

func (r *Response) AssertFunction() (*lua.LFunction, bool) { return nil, false }
func (r *Response) Peek() lua.LValue                       { return r }

func (r *Response) AssertFloat64() (float64, bool) {
	if r.Value == nil {
		return 0, false
	}

	switch r.Value.Mime {
	case "lua.LInt", "lua.LInt64", "lua.LNumber":
		n, err := r.Value.Decode()
		if err != nil {
			return 0, false
		}
		return strutil.ToFloat64(n), true

	default:
		return 0, false
	}

}

func (r *Response) AssertString() (string, bool) {
	if r.Value == nil {
		return "", false
	}

	switch r.Value.Mime {
	case "string", "[]uint8", "lua.LString":
		return r.Value.Text, true
	}
	return "", false
}

func (r *Response) text() string {
	if r.Ok() {
		return r.Value.Text
	}

	return r.Value.Text
}

func (r *Response) Elem() interface{} {
	if r.Element != nil {
		return r.Element
	}

	elem, err := r.Value.Decode()
	if err != nil {
		xEnv.Debugf("kv response %s mitm fail %v", r.Key, err)
		return lua.LNil
	}
	return elem
}

func (r *Response) Ok() bool {
	return r.Err == nil
}

func (r *Response) cnt() int {
	if r.Ok() {
		return r.Count
	}

	return 0
}

func (r *Response) ElemL(L *lua.LState) lua.LValue {
	return reflectx.ToLValue(r.Elem(), L)
}

func (r *Response) Index(L *lua.LState, key string) lua.LValue {
	switch key {
	case "ok":
		return lua.LBool(r.Ok())
	case "text":
		return lua.S2L(r.text())
	case "elem":
		return r.ElemL(L)
	case "cnt", "n":
		return lua.LInt(r.cnt())
	default:

	}
	return lua.LNil
}
