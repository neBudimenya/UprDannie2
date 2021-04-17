package main

import (
  "gorm.io/gorm"
  "gorm.io/driver/mysql"
  "time"
  "strconv"
  "net/http"
  "io/ioutil"
  "log"
)


//struct for a database connection

type dbConnection struct {
  DB *gorm.DB
}

// I use the Model instead of default gorm.Model because I'd like to change gorm and json names of columns
  type Model struct {
    ID        uint       `gorm:"primary_key auto_increment:true;column:id" json:"id"`
 CreatedAt time.Time  `gorm:"column:created_at" json:"created_at"`
 UpdatedAt time.Time  `gorm:"column:updated_at" json:"updated_at"`
 DeletedAt *time.Time `gorm:"column:deleted_at" json:"deleted_at"`
}
type Customer struct {
  Model
  FirstName string `json:"first_name"`
  SecondName string `json:"second_name"`
}
type Order struct {
  Model
  CustomerID uint     `gorm:"references:Customer"`   // Order has a customer Id
}
type OrderProduct struct {
  OrderID uint   `json:"order_id"`        // orderProduct has Order Id and Product Id
  ProductID uint `json:"product_id"`      
}
type Product struct {
  Model
  Code  string `json:"code"`
  Price uint `json:"price"`
  Amount uint
}
// struct to get information about an order by id of order
type InfoOrderProduct struct {
  OrderID uint 
  ProductID uint
  Code string
  Price uint
}
  
//func to connect to a database 
func connectToDataBase()(db *gorm.DB,err error){
  dsn := "root:root@tcp(127.0.0.1:3306)/Orders?charset=utf8mb4&parseTime=True&loc=Local"
  db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
  if err != nil {
    return nil,err
  }
  return db,nil
}
// get a products by id 
func getInfoOrderById(orderID uint)(infoOrderProduct []*InfoOrderProduct,err error){
  // amount 
  orderIdStr:= strconv.FormatUint(uint64(orderID),10)
  str  := "localhost:8090/"+orderIdStr
  resp,err := http.Get(str)
  if err != nil{
    return nil,err
  }
  defer resp.Body.Close()
  body,err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return nil,err
  }
  log.Println(string(body)) 

  db,err := connectToDataBase()
  if err != nil{
    return nil,err
  }

  db.Model(&OrderProduct{}).Select("order_products.order_id,order_products.product_id,products.code,products.price").Where("Order_id=?",orderID).Joins("join products on products.id = order_products.product_id").Scan(&infoOrderProduct)

  return infoOrderProduct,nil
}

// get all products
func getProducts() (product []*Product,err error){
  db,err := connectToDataBase()
  if err != nil{
    return nil,err
  }
 db.Model(&Product{}).Select("*").Scan(&product)
  return product,nil
}
// add new products in an order
func addProduct(orderId uint,productId uint) (err error) {
  db,err := connectToDataBase()
  if err != nil{
    return err
  }
  orderProduct := OrderProduct{OrderID: orderId, ProductID: productId}
  result := db.Create(&orderProduct)
  if result.Error != nil{
    return result.Error
  }
  return nil 
}
// delete an order 
func deleteOrder(orderId uint)(err error){
  db,err := connectToDataBase()
  if err != nil{
    return err
  }
  db.Delete(&OrderProduct{},orderId)

  return nil
}
