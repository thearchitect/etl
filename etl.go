package etl

import (
	"context"
	"log"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
)

type Empty struct{}
type TaskFunc1[In1, Out any] func(In1) Out
type TaskFunc2[In1, In2, Out any] func(In1, In2) Out

type NodeFunc func(f any, inputs ...node) node

////////////////////////////////////////////////////////////////
//// DAG
////

type DAG[InFn, OutFn any] struct {
	nodes []node
}

func NewDAG[InFn, OutFn any](build func(node NodeFunc)) *DAG[InFn, OutFn] {
	dag := &DAG[InFn, OutFn]{}

	//> Build initial DAG
	build(func(f any, inputs ...node) node {
		node := newNode(f, inputs)
		dag.nodes = append(dag.nodes, node)
		return node
	})

	return dag
}

func (dag *DAG[InFn, OutFn]) Run(ctx context.Context, inFn InFn, outFn OutFn) {
	var (
		tasks    []*task
		tasksMap = map[string]*task{}
	)

	//> Propagate tasks from nodes
	for _, node := range dag.nodes {
		t := &task{node: node}
		tasks = append(tasks, t)
		tasksMap[t.node.id] = t
	}

	for _, task := range tasks {
		for _, input := range task.node.inputs {
			//> Propagate inputs
			task.inputs = append(task.inputs, tasksMap[input])
			//> Propagate outputs
			tasksMap[input].outputs = append(tasksMap[input].outputs, task)
		}
	}

	{ //> Prepare loader inputs
		res := reflect.ValueOf(inFn).Call([]reflect.Value{})
		for _, task := range tasks {
			if task.isLoader() {
				r := res[0]
				if len(res) > 1 {
					res = res[1:]
				}
				task.premadeInput = r
			}
		}
	}

	var exit = &task{}

	{ //> Add exit task
		exit.node.id = "exit"
		exit.node.sn = "exit"

		//> Create fake exit func
		exit.node.v = reflect.ValueOf(outFn)

		//> Connect the dots
		for _, task := range tasks {
			if task.isExporter() {
				exit.inputs = append(exit.inputs, task)
				task.outputs = append(task.outputs, exit)
			}
		}
	}

	//> !
	exit.run()
}

////////////////////////////////////////////////////////////////
//// Task
////

type task struct {
	node    node
	inputs  []*task
	outputs []*task

	premadeInput reflect.Value

	runLock sync.Mutex
	done    bool
	result  taskResult
}

type taskResult struct {
	value reflect.Value
	error error
}

func (t *task) isLoader() bool      { return len(t.inputs) == 0 }
func (t *task) isTransformer() bool { return len(t.inputs) > 0 && len(t.outputs) > 0 }
func (t *task) isExporter() bool    { return len(t.outputs) == 0 }

func (t *task) run() taskResult {
	{ //> Prevent multiple executions of a task
		t.runLock.Lock()
		defer t.runLock.Unlock()
		defer func() { t.done = true }()
		if t.done {
			return t.result
		}
	}

	var inputsResults []reflect.Value

	if t.isLoader() {
		inputsResults = []reflect.Value{t.premadeInput}
	} else {
		//> Run all the deps and wait for it
		inputsResults = make([]reflect.Value, len(t.inputs))
		var (
			wg sync.WaitGroup
		)
		for i, in := range t.inputs {
			wg.Add(1)
			go func(i int, in *task) {
				defer wg.Done()

				res := in.run()
				if res.error != nil {
					panic(res.error)
				}
				inputsResults[i] = res.value
			}(i, in)
		}
		wg.Wait()
	}

	{ //> Execute the task itself
		res := t.node.v.Call(inputsResults)
		t.result = taskResult{
			error: nil,
		}
		if len(res) > 0 {
			t.result.value = res[0]
		}
		return t.result
	}
}

////////////////////////////////////////////////////////////////
//// Node
////

type node struct {
	t reflect.Type
	v reflect.Value

	fn string
	sn string
	dn string
	id string

	inputs []string
}

func newNode(f any, inputs []node) node {
	n := node{}

	for _, input := range inputs {
		n.inputs = append(n.inputs, input.id)
	}

	n.t = reflect.TypeOf(f)
	n.v = reflect.ValueOf(f)
	fun := runtime.FuncForPC(n.v.Pointer())
	n.fn = fun.Name()
	n.sn = n.fn[strings.LastIndex(n.fn, "/")+1:]
	n.dn = n.sn + " " + n.t.String()[4:]

	{
		var (
			stack = string(debug.Stack())
			spl   = strings.Split(stack, "\n")
			fll   = spl[8]
		)
		n.id = fll[:strings.LastIndex(fll, " ")]
	}

	log.Println(n.dn, n.id)

	return n
}
