FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/mock-server .

FROM alpine:3.20
COPY --from=build /out/mock-server /usr/local/bin/mock-server
ENTRYPOINT ["/usr/local/bin/mock-server"]
