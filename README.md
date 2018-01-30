

# Dockerfile build behind a proxy

```sh

# Values for the C&S proxy
proxy_host=172.27.128.34
proxy_port=3128


docker build \
        --build-arg HTTP_PROXY_HOST=$proxy_host \
        --build-arg HTTPS_PROXY_HOST=$proxy_host \
        --build-arg HTTP_PROXY_PORT=$proxy_port \
        --build-arg HTTPS_PROXY_PORT=$proxy_port \
        -t ikatsbase .
```
