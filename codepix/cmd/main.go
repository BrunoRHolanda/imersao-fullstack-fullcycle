package main

import (
	"github.com/BrunoRHolanda/imersao-fullstack-fullcycle/codepix/infrastructure/db"
	"github.com/BrunoRHolanda/imersao-fullstack-fullcycle/codepix/application/grpc"
	"github.com/jinzhu/gorm"
	"os"
)

var database *gorm.DB

func main() {
	database = db.ConnectDB(os.Getenv("env"))
}
