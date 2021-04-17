package main 

import (
  "log"
  "net/http"
  "os"
  "os/signal"
  "time"
  "context"
)

func main(){

  l := log.New(os.Stdout,"orders-api",log.LstdFlags)
  
  //create a server Mux
  sm := http.NewServeMux()

  //create a server handler what contains many other handlers (handlers.go)
  handler := NewHandler(l)
  // use just created handler
  sm.Handle("/",handler)

  // create my server with my configuratiion and Mux, also don't forget about timeouts
  server := &http.Server{
    Addr: ":8080",
    Handler: sm,
    IdleTimeout: 120*time.Second,
    ReadTimeout: 1*time.Second,
    WriteTimeout: 1*time.Second,
  }
  // if the program will receive an os signal to kill or interrupt it will "gracefully" shutdown and won't be just killed
  go func(){ 
    err := server.ListenAndServe()
    if err != nil{
      l.Fatal(err)
    }
  }()

  sigChan := make(chan os.Signal)
  signal.Notify(sigChan,os.Interrupt)
  signal.Notify(sigChan,os.Kill)
  
  sig := <-sigChan
  l.Println("Received terminate, graceful shutdown",sig)

  timeOutContext,_ := context.WithTimeout(context.Background(),30*time.Second)

  server.Shutdown(timeOutContext)
}
