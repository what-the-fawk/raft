FROM golang:1.22

WORKDIR /app
COPY . ./
RUN go mod download
RUN cd runner
RUN ls
WORKDIR /app/runner
RUN go build -o main .
ENTRYPOINT ["/app/runner/main"]