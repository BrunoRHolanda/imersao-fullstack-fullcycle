package main

import (
	"fmt"

	"github.com/brunorholanda/imersao-full-cicle/desafios-go/Desafio_1"
)

func main() {
	err, user := Desafio_1.NewUser("Bruno", "brunorodriguesholanda@gmail.com")

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(user)
}
