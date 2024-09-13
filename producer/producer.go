package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/template/html/v2"
)

type Comment struct {
	Card       string `json:"card"`
	Expiration string `json:"expiration"`
	Name       string `json:"name"`
	Code       string `json:"code"`
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func PushCommentToQueue(topic string, message []byte) error {
	brokersUrl := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func main() {
	viewsEngine := html.New("../templates", ".html")
	app := fiber.New(fiber.Config{Views: viewsEngine})
	app.Get("/", func(c *fiber.Ctx) error {
		return c.Render("index", nil)
	})
	api := app.Group("/api")
	api.Get("/", func(c *fiber.Ctx) error {
		return c.Render("create", nil)
	})
	api.Post("/", createComment)
	app.Listen(":3000")
}

func createComment(c *fiber.Ctx) error {
	cmt := new(Comment)
	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}
	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}
	//Используем функция для отправки в кафку
	PushCommentToQueue("comments", cmtInBytes)

	return confirmationView(c)
}

func confirmationView(c *fiber.Ctx) error {
	err := c.Render("waiting", nil)
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error rendering view",
		})
		return err
	}
	return err
}
