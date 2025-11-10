FROM gcr.io/distroless/cc-debian13
COPY ducktape /ducktape
ENTRYPOINT ["/ducktape"]
