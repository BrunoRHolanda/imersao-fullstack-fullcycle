package grpc

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"google.golang.org/grpc"
	"log"
	"net"
)

func StartGrpcServer(database *gorm.DB, port int) {
	grpcServer := grpc.NewServer()

	address := fmt.Sprintf("0.0.0.0:#{port}")
	listner, err := net.Listen("tcp", address)

	if err != nil {
		log.Fatal("cannot start grpc server ", err)
	}

	log.Printf("gRPC server has been started on port %d", port)
	err = grpcServer.Serve(listner)

	if err != nil {
		log.Fatal("cannot start grpc server", err)
	}
}
