FROM golang:1.25-alpine AS builder
ARG GITHUB_TOKEN
ARG GOPRIVATE=github.com/Endea4/*
RUN apk add --no-cache git ca-certificates gcc musl-dev
RUN git config --global url."https://${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"
ENV GOPRIVATE=${GOPRIVATE}
ENV CGO_ENABLED=1

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /customer-bot ./cmd

FROM alpine:3.21
RUN apk --no-cache add ca-certificates
COPY --from=builder /customer-bot /customer-bot
EXPOSE 9088
CMD ["/customer-bot"]
