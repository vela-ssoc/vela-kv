package kv

import (
	"github.com/vela-ssoc/vela-kit/lua"
	"github.com/vela-ssoc/vela-kit/vela"
)

var xEnv vela.Environment

func bucketL(L *lua.LState) int {
	name := L.IsString(1)
	bkt := &Bucket{
		Name: name,
		Max:  1024 * 1024 * 2, //2M
	}

	L.Push(bkt)
	return 1
}

func WithEnv(env vela.Environment) {
	xEnv = env
	xEnv.Mime(List{}, ListEncode, ListDecode)

	tab := lua.NewUserKV()
	xEnv.Set("kv", lua.NewExport("vela.kv.export", lua.WithTable(tab), lua.WithFunc(bucketL)))
}
