FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/notification ./cmd/notification

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/notification /app/notification
EXPOSE 8090 9090 9290
ENTRYPOINT ["/app/notification"]