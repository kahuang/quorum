package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/kahuang/quorum"
)

func main() {
	config, err := quorum.NewDefaultConfig([]string{"10.10.2.224"})
	if err != nil {
		log.Fatalf(err.Error())
	}
	quorumNode, err := quorum.NewQuorumNode(config)
	if err != nil {
		log.Fatalf(err.Error())
	}
	httpApi := quorum.NewHTTPApi(quorumNode)
	router := httprouter.New()
	httpApi.Start(router)
	srv := &http.Server{Addr: ":" + config.Port, Handler: router}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("Error while serving: %v", err)
		}
	}()
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan
	log.Printf("Shutting down...")
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Second)
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Error while shutting down: %v", err)
	}
	quorumNode.Shutdown(context.Background())
}
