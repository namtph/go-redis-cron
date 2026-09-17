FROM golang:1.23-alpine AS build

WORKDIR /repo
COPY go.mod go.sum ./
COPY gorediscron ./gorediscron
COPY ui ./ui
COPY examples/two-pods-full ./examples/two-pods-full

WORKDIR /repo/examples/two-pods-full
RUN go mod download && CGO_ENABLED=0 go build -o /out/two-pods-full .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/two-pods-full /usr/local/bin/two-pods-full
ENTRYPOINT ["/usr/local/bin/two-pods-full"]
