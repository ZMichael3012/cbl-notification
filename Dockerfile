# Build context: services/ (access to the replace directives on ../cbl-kit and ../cbl-protos is required).
# Command: docker build -f cbl-notification/Dockerfile -t cbl/notification:dev .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY cbl-kit/ /src/cbl-kit/
COPY cbl-protos/ /src/cbl-protos/
COPY cbl-notification/ /src/cbl-notification/
WORKDIR /src/cbl-notification
RUN go mod download
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/notification ./cmd/notification

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/notification /app/notification
COPY --from=build /src/cbl-notification/migrations /app/migrations
EXPOSE 8090 9090 9290
ENTRYPOINT ["/app/notification"]
