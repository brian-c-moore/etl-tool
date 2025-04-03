# Use the official Golang image with Go 1.23 installed.
FROM golang:1.23

# Set the working directory inside the container.
WORKDIR /go/src/etl-tool

# Copy your entire project into the container.
COPY . .

# Ensure your module dependencies are tidy.
RUN go mod tidy

# Run your tests with caching disabled (-count=1) for a clean build each time.
CMD ["go", "test", "-v", "-count=1", "./..."]

