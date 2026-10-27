FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /gedis ./cmd/gedis

FROM scratch
COPY --from=build /gedis /gedis
VOLUME /data
EXPOSE 6380
ENTRYPOINT ["/gedis", "-addr", ":6380", "-aof", "/data/appendonly.aof"]
