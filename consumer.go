package saramaconsumergo

import (
	"log"
	"os"
	"strings"

	mapsutils "github.com/AchmadRifai/maps-utils"
	"github.com/IBM/sarama"
)

type Consumer struct {
	config   *sarama.Config
	brokers  []string
	topics   []string
	master   sarama.Consumer
	handlers map[string]func(*sarama.ConsumerMessage)
}

func (c *Consumer) Execute() {
	master, err := sarama.NewConsumer(c.brokers, c.config)
	defer masterError(master)
	if err != nil {
		panic(err)
	}
	c.master = master
	consumers, errors := c.consume()
	signals := make(chan os.Signal, 1)
	doneCh := make(chan struct{})
	go func() {
		for {
			defer normalError()
			select {
			case msg := <-consumers:
				log.Println("Message", string(msg.Value), "From topic", msg.Topic)
				c.handlers[msg.Topic](msg)
			case err := <-errors:
				panic(err)
			case <-signals:
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
}

func (c *Consumer) consume() (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
	consumers := make(chan *sarama.ConsumerMessage)
	errors := make(chan *sarama.ConsumerError)
	for _, topic := range c.topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}
		log.Println("Start consuming topic", topic)
		defer normalError()
		partition, err := c.master.Partitions(topic)
		if err != nil {
			panic(err)
		}
		consumer, err := c.master.ConsumePartition(topic, partition[0], sarama.OffsetOldest)
		if err != nil {
			panic(err)
		}
		go func(topic string, consumer sarama.PartitionConsumer) {
			for {
				select {
				case consumerError := <-consumer.Errors():
					errors <- consumerError
				case msg := <-consumer.Messages():
					consumers <- msg
					log.Println("Got message on", topic)
				}
			}
		}(topic, consumer)
	}
	return consumers, errors
}

func (c *Consumer) GetConfig() *sarama.Config {
	return c.config
}

func NewConsumer(brokers []string, handlers map[string]func(*sarama.ConsumerMessage)) *Consumer {
	config := sarama.NewConfig()
	config.ClientID = "go-kafka-consumer"
	config.Consumer.Return.Errors = true
	topics := mapsutils.KeysOfMap(handlers)
	return &Consumer{config: config, brokers: brokers, topics: topics, handlers: handlers}
}
