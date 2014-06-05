package main

import (
	"code.google.com/p/go.exp/fsnotify"
	"github.com/streadway/amqp"

	"bufio"
	"log"
	"os"
	"text/scanner"
	"time"
)

const LOG_FILE_NAME = "/var/spool/scribe/events/log-events_current"

const EX_NAME = "test_ex"

// const RURL = "amqp://ev-guest:ev-guest@dev-datapi-s1.wikia-prod/events"
const RURL = "amqp://ev-guest:ev-guest@10.8.68.187/events"

func publisher(amqp_chanel *amqp.Channel, msg_queue chan string) {
	for raw_msg := range msg_queue {
		msg := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			ContentType:  "application/json",
			Body:         []byte(raw_msg),
		}
		// log.Printf("%+v\n", msg)
		amqp_chanel.Publish(EX_NAME, "", false, false, msg)
	}
}

func reader(msg_queue chan string, read_trigger chan bool) {
	file, err := os.Open(LOG_FILE_NAME)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()
	reader := bufio.NewReader(file)

	var s scanner.Scanner
	s.Init(reader)
	for trigger := range read_trigger {
		log.Printf("trigger %+v", trigger)
		if trigger {
			line, err := reader.ReadString('\n')
			for err == nil {
				// log.Prixntf("%+v %+v\n", line, err)
				msg_queue <- line
				line, err = reader.ReadString('\n')
			}
		} else {
			return
		}
	}

}

func watch(msg_queue chan string) {
	log.Print("w start")
	defer log.Print("w exit")

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	err = watcher.Watch(LOG_FILE_NAME)
	if err != nil {
		log.Print(err)
		return
	}
	read_trigger := make(chan bool, 3000)
	go reader(msg_queue, read_trigger)
	read_trigger <- true

	for {
		select {
		case ev := <-watcher.Event:
			log.Printf("EV %+v", ev)
			if ev.IsModify() || ev.IsCreate() {
				read_trigger <- true
			} else {
				read_trigger <- true
				read_trigger <- false
				return
			}
		case err := <-watcher.Error:
			log.Fatal(err)
		}
	}
}

func main() {
	connection, err := amqp.Dial(RURL)
	if err != nil {
		z, _ := amqp.ParseURI(RURL)
		log.Printf("%+v\n", z.Vhost)
		log.Fatal(err)
	}

	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		log.Fatal(err)
	}

	ch := make(chan string, 10000)
	go publisher(channel, ch)
	// go watch(ch)

	// t := time.NewTicker(time.Second)
	// or just use the usual for { select {} } idiom of receiving from a channel
	for 2*2 == 4 {
		watch(ch)
	}
}
