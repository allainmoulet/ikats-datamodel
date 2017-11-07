FROM maven:3.3.9-jdk-8
MAINTAINER Germain GAU <germain.gau@c-s.fr>

#########################################################
#
# TODO: Split this image into a multi stage build one
#       as soon as war repacking won't be necessary
#
#########################################################

ARG HTTP_PROXY_HOST=""
ARG HTTP_PROXY_PORT=""
ARG HTTPS_PROXY_HOST=""
ARG HTTPS_PROXY_PORT=""

ADD . /srcs
WORKDIR /srcs/ikats-main
RUN mvn package \
  -Dtarget=template \
  -DskipTests=true \
  -Dhttp.proxyHost=$HTTP_PROXY_HOST \
  -Dhttp.proxyPort=$HTTP_PROXY_PORT \
  -Dhttps.proxyHost=$HTTPS_PROXY_HOST \
  -Dhttps.proxyPort=$HTTPS_PROXY_PORT

CMD [ \
  "bash", \
  "/srcs/docker/container_init.sh", \
  "/srcs/", \
  "/srcs/ikats-main/target/template/TemporalDataManagerWebApp.war" \
]
