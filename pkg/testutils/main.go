package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"entrogo.com/entroq"
	"entrogo.com/entroq/grpc"
	"github.com/golang/protobuf/jsonpb"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
)

var (
	eq      *entroq.EntroQ
	svcAddr string
	queue   string
	insArgs []entroq.InsertArg
)

func main() {
	flag.StringVar(&svcAddr, "svc", ":37706", "address of service.  Uses :37706 as default")
	flag.StringVar(&queue, "queue", "/task/fusion/outbox", "queue into which to insert the test task")

	var err error
	flag.Parse()

	eq, err = entroq.New(context.Background(), grpc.Opener(svcAddr, grpc.WithInsecure()))
	if err != nil {
		log.Fatalf("Error starting entroq %v", err)
	}

	newTask := &pb.FusionTask{
		DoneQueue:   "None",
		FuserId:     []string{"noFuser"},
		DetectionId: "1234-5678",
	}
	j, err := new(jsonpb.Marshaler).MarshalToString(newTask)
	fmt.Printf("%v+\n\n", j)
	if err != nil {
		log.Fatalf("Marhsaling proto to string: %v", err)
	}
	insArgs = append(insArgs, entroq.WithValue([]byte(j)))
	ins, _, err := eq.Modify(context.Background(), entroq.InsertingInto(queue, insArgs...))

	b, err := json.MarshalIndent(ins, "", "\t")
	if err != nil {
		log.Fatalf("insert json: %v", err)
	}
	fmt.Println(string(b))
}
