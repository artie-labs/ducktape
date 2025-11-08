FROM alpine:3.22
RUN apk add --no-cache tzdata
COPY ducktape /ducktape
ENTRYPOINT ["/ducktape"]
