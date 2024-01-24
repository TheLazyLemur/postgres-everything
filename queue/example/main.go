package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/thelazylemur/postgres-everything/queue"
)

var (
	isPublisher = flag.Bool("publisher", false, "Enqueue")
	queueName   = flag.String("queue", "payments", "Queue name")
)

var (
	lock = &sync.Mutex{}
	msgs = make(map[string]struct{})
)

func writeToMap(s string) error {
	lock.Lock()
	defer lock.Unlock()

	if _, ok := msgs[s]; ok {
		return errors.New("key already exists")
	}

	msgs[s] = struct{}{}
	return nil
}

const (
	user     = "postgres"
	password = "postgres"
	host     = "localhost"
	port     = 5432
	dbname   = "postgres"
	sslmode  = "disable"
)

func main() {
	flag.Parse()

	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	conString := fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=%s", user, password, host, port, dbname, sslmode)

	c, err := queue.NewQueue(conString, "queue", *queueName)
	if err != nil {
		log.Fatal(err)
	}

	defer c.Close()

	numMessages := 20000

	if *isPublisher {
		ctx := context.Background()
		startTime := time.Now()

		for i := 0; i < numMessages; i++ {
			id := i + 1
			tx, err := c.GetTx()
			if err != nil {
				panic(err)
			}

			err = c.EnqueueTx(ctx, tx, []byte(fmt.Sprintf("Payment sent %d", id)))
			if err != nil {
				_ = tx.Rollback()
				continue
			}

			_ = tx.Commit()
		}

		elapsedTime := time.Since(startTime)
		messagesPerSecond := float64(numMessages) / elapsedTime.Seconds()
		fmt.Printf("Processed %d messages in %s\n", numMessages, elapsedTime)
		fmt.Printf("Messages per second: %.2f\n", messagesPerSecond)
	}

	if !*isPublisher {
		wg := &sync.WaitGroup{}
		wg.Add(2)

		for i := 0; i < 2; i++ {
			go func() {
				defer wg.Done()
				ctx := context.Background()
				uid := uuid.New()

				noMsgs := 0

				for {
					tx, err := c.GetTx()
					if err != nil {
						panic(err)
					}

					msg, err := c.DequeueTx(ctx, tx)
					if err != nil && err == sql.ErrNoRows {
						_ = tx.Commit()
						time.Sleep(5 * time.Second)
						noMsgs++
						fmt.Println(noMsgs)
						if noMsgs == 5 {
							break
						}
						continue
					} else if err != nil {
						_ = tx.Rollback()
						continue
					}

					noMsgs = 0
					err = writeToMap(string(msg))
					if err != nil {
						_ = tx.Rollback()
						fmt.Println(err)
						continue
					} else {
						_ = tx.Commit()
					}

					time.Sleep(10 * time.Millisecond)
					fmt.Println(uid, string(msg))
				}
			}()
		}
		wg.Wait()

		fmt.Println(len(msgs))
	}
}
