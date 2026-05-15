FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/processor ./cmd/processor
RUN go build -o /out/producer ./cmd/producer

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/processor /processor
COPY --from=builder /out/producer /producer
COPY schemas/ /schemas/
