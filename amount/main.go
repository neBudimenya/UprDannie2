package main

import (
  "gorm.io/driver/postgres"
  "gorm.io/gorm"
  "log"
  "encoding/json"
  "time"
  "github.com/streadway/amqp"
  "strconv"

)
func connectToDataBase()(db *gorm.DB,err error){
  dsn := "host=localhost user=postgres password=123 dbname=warehouse port=5432 sslmode=disable"
  db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
  if err != nil {
    return nil,err
  }
  return db,nil
}

  type Model struct {
 ID        uint       `gorm:"primary_key auto_increment:true;column:id" json:"id"`
 CreatedAt time.Time  `gorm:"column:created_at" json:"-"`
 UpdatedAt time.Time  `gorm:"column:updated_at" json:"-"`
 DeletedAt *time.Time `gorm:"column:deleted_at" json:"-"`
}
type Product struct {
  Model    // changed gorm.model
  Amount uint       `gorm:"column:amount" json:"amount"`

}
// a method to get product amount 


func getProductAmount(orderId uint)(product []*Product,err error){
 db,err := connectToDataBase()
  if err != nil {
    return nil,err
  }
  db.Model(&Product{}).Select("products.id,products.amount").Where("products.id = ?",orderId).Scan(&product)
  if db.Error != nil {
    return nil, db.Error
  }
  return product,nil
}

func main() {
        conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
        if err != nil {
          log.Fatal(err)
        }
        defer conn.Close()

        ch, err := conn.Channel()
        if err != nil {
          log.Fatal(err)
        }
        defer ch.Close()

        q, err := ch.QueueDeclare(
                "rpc_queue", // name
                false,       // durable
                false,       // delete when unused
                false,       // exclusive
                false,       // no-wait
                nil,         // arguments
        )
        if err != nil {
          log.Fatal(err)
        }

        err = ch.Qos(
                1,     // prefetch count
                0,     // prefetch size
                false, // global
        )
        if err != nil {
          log.Fatal(err)
        }

        msgs, err := ch.Consume(
                q.Name, // queue
                "",     // consumer
                false,  // auto-ack
                false,  // exclusive
                false,  // no-local
                false,  // no-wait
                nil,    // args
        )
        if err != nil {
          log.Fatal(err)
        }

        forever := make(chan bool)

        go func() {
                for d := range msgs {
                        //n := d.Body
                        if err != nil {
                        log.Fatal(err)
                        }

                        strUint,err := strconv.ParseUint(string(d.Body),10,32)
                        
                        
                        response,err := getProductAmount(uint(strUint))
                        if err != nil {
                        log.Fatal(err)
                        }
                        responseJson,err := json.Marshal(response)
                        if err != nil {
                        log.Fatal(err)
                        }
                 
                  
                  corrId := randomString(32)  //create a random corrId

                        err = ch.Publish(
                                "",        // exchange
                                d.ReplyTo, // routing key
                                false,     // mandatory
                                false,     // immediate
                                amqp.Publishing{
                                        ContentType:   "text/plain",
                                        CorrelationId: d.CorrelationId,
                                        Body:          responseJson,
                                })

                        if err != nil {
                          log.Fatal(err)
                        }

                        d.Ack(false)
                }
        }()

        log.Printf(" [*] Awaiting RPC requests")
        <-forever
}


