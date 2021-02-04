package grpc

import (
	"fmt"
	"github.com/BrunoRHolanda/imersao-fullstack-fullcycle/codepix/application/grpc/pb"
	"github.com/BrunoRHolanda/imersao-fullstack-fullcycle/codepix/application/usecase"
	"github.com/BrunoRHolanda/imersao-fullstack-fullcycle/codepix/infrastructure/repository"
	"github.com/jinzhu/gorm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
)

func StartGrpcServer(database *gorm.DB, port int) {
	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)

	pixRepository := repository.PixKeyRepositoryDb{Db: database}
	pixUseCase := usecase.PixUseCase{PixKeyRepository: pixRepository}
	pixGrpcService := NewGrpcService(pixUseCase)

	pb.RegisterPixServiceServer(grpcServer, pixGrpcService)

	address := fmt.Sprintf("0.0.0.0:%d", port)
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
