package main

import (
  "testing"
  "net/http"
  "net/http/httptest"
  "log"
  "os"
  "encoding/json"
)

func TestHandler(t *testing.T){
  req,err := http.NewRequest("GET","http://localhost:8080/",nil)
  if err != nil{
    t.Fatalf("could not create request: %v",err)
  }
  rec := httptest.NewRecorder()


  l := log.New(os.Stdout,"orders-api",log.LstdFlags)
  handler := NewHandler(l)
  handler.ServeHTTP(rec,req)


  result :=rec.Result()
  if result.StatusCode != http.StatusOK{
    t.Errorf("excepted status OK; got %v  and body: ",result.Status)
  }

  var prod []Product
  if err := json.NewDecoder(result.Body).Decode(&prod); err != nil{
    t.Fatalf("decoder err %v",err)
  }
}

func TestHandlerGetOrderById(t *testing.T){
  req,err := http.NewRequest("GET","http://localhost:8080/1",nil)
  if err != nil{
    t.Fatalf("could not create request: %v",err)
  }
  rec := httptest.NewRecorder()


  l := log.New(os.Stdout,"orders-api",log.LstdFlags)
  handler := NewHandler(l)
  handler.ServeHTTP(rec,req)


  result :=rec.Result()
  if result.StatusCode != http.StatusOK{
    t.Errorf("excepted status OK; got %v  and body: ",result.Status)
  }

  var prod []Product
  if err := json.NewDecoder(result.Body).Decode(&prod); err != nil{
    t.Fatalf("decoder err %v",err)
  }
}
func TestHandlerPost(t *testing.T){
  req,err := http.NewRequest("POST","http://localhost:8080/",nil)
  if err != nil{
    t.Fatalf("could not create request: %v",err)
  }
  rec := httptest.NewRecorder()


  l := log.New(os.Stdout,"orders-api",log.LstdFlags)
  handler := NewHandler(l)
  handler.ServeHTTP(rec,req)


  result :=rec.Result()
  if result.StatusCode != http.StatusBadRequest{
    // response status should be bad request because the api doesn't handle put method
    t.Fatalf("Request should be bad request, but got: %v,%v",result.Status,err)
  }

}
func TestHandlerDelete(t *testing.T){
  req,err := http.NewRequest("DELETE","http://localhost:8080/",nil)
  if err != nil{
    t.Fatalf("could not create request: %v",err)
  }
  rec := httptest.NewRecorder()


  l := log.New(os.Stdout,"orders-api",log.LstdFlags)
  handler := NewHandler(l)
  handler.ServeHTTP(rec,req)


  result :=rec.Result()
  if result.StatusCode != http.StatusBadRequest{
    // response status should be bad request because we chose method delete, but didn't chose any id
    t.Fatalf("Request should be bad request, but got: %v,%v",result.Status,err)
  }

}
func TestHandlerPut(t *testing.T){
  req,err := http.NewRequest("PUT","http://localhost:8080/1?id=2",nil)
  if err != nil{
    t.Fatalf("could not create request: %v",err)
  }
  rec := httptest.NewRecorder()


  l := log.New(os.Stdout,"orders-api",log.LstdFlags)
  handler := NewHandler(l)
  handler.ServeHTTP(rec,req)


  result :=rec.Result()
  if result.StatusCode != http.StatusOK{
    // response status should be bad request because we chose method delete, but didn't chose any id
    t.Fatalf("Request should be bad request, but got: %v,%v",result.Status,err)
  }

}
