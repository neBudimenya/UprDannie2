package main

import (
  "gorm.io/driver/postgres"
  "gorm.io/gorm"
  "log"
  "encoding/json"
  "time"
  "github.com/streadway/amqp"
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


func getProductAmount()(product []*Product,err error){

 db,err := connectToDataBase()
  if err != nil {
    return nil,err
  }
  db.Model(&Product{}).Select("products.id,products.amount").Scan(&product)
  if db.Error != nil {
    return nil, db.Error
  }
  return product,nil
}

func main (){
  conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
  if err != nil {
    log.Fatal(err)
  }
  channel,err := conn.Channel()
  if err != nil{
    log.Fatal(err)
  }
  defer channel.Close()
  q, err := channel.QueueDeclare(
    "amount",
    false,
    false,
    false,
    false,
    nil,
  )
  if err != nil{
    log.Fatal(err)
  }

  productAmount,err := getProductAmount()
  if err != nil {
    log.Fatal(err)
  }
  body,err := json.Marshal(productAmount)
  if err != nil {
    log.Fatal(err)
  }
  err = channel.Publish(
    "",
    q.Name,
    false,
    false,
    amqp.Publishing{
      ContentType:"text/plain",
      Body:       []byte(body),
    })
    if err != nil {
      log.Fatal(err)
    }
    log.Printf("[x] Sent %s",body)
  
}


