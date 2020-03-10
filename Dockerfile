FROM golang:alpine

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o main .

EXPOSE 8000

RUN chmod u+x main

ENTRYPOINT ["./main"]