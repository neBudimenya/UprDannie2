package main

import (
  "log"
  "net/http"
  "encoding/json"
  "strconv"
  "strings"
)

// an useful struct to create a logger. It helps to make the dependencies with clear logic of handlers
type Handler struct {
  l *log.Logger
}

func NewHandler(l *log.Logger) *Handler{
  return &Handler{l}
}

// this is a main handler, it will give other handlers depend of a method(request)
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request){
  if r.Method == http.MethodGet{
    url := r.URL.Path
    if url == "/"{
    // if url doesn't contain any id then get every order
     getEveryOrderHandler(w,r)
     return

    }  else {
    // otherwise get an order info by id
      tmp := strings.Trim(url,"/")
      orderIdConv,err := strconv.ParseUint(tmp,10,64)
      if err != nil{
        http.Error(w,"failed to convert URL Path in uint",http.StatusInternalServerError)
      }
      getOrderByIdHandler(uint(orderIdConv),w,r)
    }
  }
  if r.Method == http.MethodPost{
    // the method won't do anything, because I don't have to create a new order 
      http.Error(w,"there is nothing that you can do with a post method",http.StatusBadRequest)
      return
  }
  if r.Method == http.MethodPut{
    url := r.URL.Path

    if url == "/"{
      /// if url doesn't contain any id then return error
        http.Error(w,"Method put but you didn't enter any id",http.StatusBadRequest)
        return
    }  else {
    // otherwise delete an order by id
      tmp := strings.Trim(url,"/")
      orderIdConv,err := strconv.ParseUint(tmp,10,64)
      if err != nil{
        http.Error(w,"failed to convert URL Path in uint",http.StatusInternalServerError)
      }
      //convert product id to uint 
      productId := r.URL.Query().Get("id")
      productId_uint,err := strconv.Atoi(productId)
      if err != nil{
        http.Error(w,"failed to convert URL Path in uint",http.StatusInternalServerError)
      }

    putNewOrderHandler(uint(orderIdConv),uint(productId_uint),w,r)
    return
  }
}
  
  if r.Method == http.MethodDelete{
    url := r.URL.Path
    if url == "/"{
      /// if url doesn't contain any id then return error
        http.Error(w,"Method  delete but you didn't enter any id",http.StatusBadRequest)
        return
    }  else {
    // otherwise delete an order by id
      tmp := strings.Trim(url,"/")
      orderIdConv,err := strconv.ParseUint(tmp,10,64)
      if err != nil{
        http.Error(w,"failed to convert URL Path in uint",http.StatusInternalServerError)
      }
      deleteOrderByIdHandler(uint(orderIdConv),w,r)
    return
  }
     
  
}
}

func putNewOrderHandler(orderId uint,productId uint,w http.ResponseWriter, r *http.Request){
  err := addProduct(orderId,productId)
     if err != nil{
        http.Error(w,"error in put new order handler",http.StatusInternalServerError)
     }
     w.WriteHeader(http.StatusOK)
}
// a handler to delete an order by Id
func deleteOrderByIdHandler(OrderId uint,w http.ResponseWriter, r *http.Request){
     err := deleteOrder(OrderId)
     if err != nil{
        http.Error(w,"failed to delete an Order Info by Id",http.StatusInternalServerError)
     }
     w.WriteHeader(http.StatusOK)
     


}
/// a handler to get an order by id 
func getOrderByIdHandler(OrderId uint,w http.ResponseWriter, r *http.Request){
     lp,err := getInfoOrderById(OrderId)
     if err != nil{
        http.Error(w,"failed to get an Order Info by Id",http.StatusInternalServerError)
     }
     lp_json,err := json.Marshal(lp)
     if err != nil{
         http.Error(w,"failed to marshal json",http.StatusInternalServerError)
      }
     w.WriteHeader(http.StatusOK)
      w.Write(lp_json)
}
// handler to get information about every order, if client didn't give any information about orders and used a method get. It's like a default response by a server
func getEveryOrderHandler(w http.ResponseWriter, r *http.Request){
  lp,err := getProducts()
  if err != nil{
    http.Error(w,"Failed to get every order",http.StatusInternalServerError)
  }
  lp_json,err := json.Marshal(lp)
  if err != nil{
    http.Error(w,"failed to marshal json",http.StatusInternalServerError)
  }
  w.WriteHeader(http.StatusOK)
  w.Write(lp_json)

}
