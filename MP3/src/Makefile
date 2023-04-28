all: server client

server: server_server/server.go
	go build -race server_server/server.go

client: client_client/client.go
	go build -race client_client/client.go