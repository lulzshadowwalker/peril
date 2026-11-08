default:
	just --list

server:
	go run cmd/server/main.go

client:
	go run cmd/client/main.go

start:
	./rabbit.sh start

stop:
	./rabbit.sh stop

logs:
	./rabbit.sh logs
