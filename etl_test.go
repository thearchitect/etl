package etl_test

import (
	"context"
	"log"
	"reflect"
	"testing"

	"github.com/thearchitect/ashram/ashram-backend/experiments/etl"
)

// https://github.com/polarsignals/frostdb
// https://github.com/kelindar/column

type Ares struct{}

func A(string) Ares {
	return Ares{}
}

type Bres struct{}

func B(string) Bres {
	return Bres{}
}

type Cres struct{}

func C(Ares, Bres) Cres {
	return Cres{}
}

type Dres struct{}

func D(Ares, Bres) Dres {
	return Dres{}
}

func E(Cres, Dres) int8 {
	return 0
}

func F(Ares, Cres, Dres) int8 {
	return 1
}

func TestName(t *testing.T) {
	log.SetFlags(log.Lmicroseconds)

	dag := etl.NewDAG[
		func() (string, string),
		func(int8, int8),
	](func(node etl.NodeFunc) {
		nA := node(A)
		nB := node(B)
		nC := node(C, nA, nB)
		nD := node(D, nA, nB)
		node(E, nC, nD)
		node(F, nA, nC, nD)
	})

	log.Println(reflect.TypeOf(etl.Empty{}))

	dag.Run(context.Background(), func() (inA string, inB string) {
		return "", ""
	}, func(resE int8, resF int8) {
		log.Println("result", resE, resF)
	})
}
