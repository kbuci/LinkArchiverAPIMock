FROM golang:1.12-alpine

# Download and install the latest release of dep
ADD https://github.com/golang/dep/releases/download/v0.4.1/dep-linux-amd64 /usr/bin/dep
RUN chmod +x /usr/bin/dep
RUN apk add --update --no-cache alpine-sdk bash ca-certificates \
      libressl \
      tar \
      git openssh openssl yajl-dev zlib-dev cyrus-sasl-dev openssl-dev build-base coreutils
WORKDIR /root
RUN git clone https://github.com/edenhill/librdkafka.git
WORKDIR /root/librdkafka
RUN /root/librdkafka/configure
RUN make
RUN make install

# Copy the code from the host and compile it
WORKDIR $GOPATH/src/github.com/kbuci/text-hosting-mock
COPY Gopkg.toml Gopkg.lock ./
RUN dep ensure --vendor-only
COPY . ./
RUN mkdir archive
RUN mkdir temp

EXPOSE 8080