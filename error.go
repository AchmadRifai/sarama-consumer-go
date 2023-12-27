package saramaconsumergo

import (
	"log"
	"runtime/debug"

	"github.com/IBM/sarama"
)

func masterError(master sarama.Consumer) {
	normalError()
	if err := master.Close(); err != nil {
		panic(err)
	}
}

func normalError() {
	if r := recover(); r != nil {
		log.Println("Error", r)
		log.Println("Stack trace", string(debug.Stack()))
	}
}
