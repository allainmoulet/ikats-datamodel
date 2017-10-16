FROM maven:3.3.9-jdk-8
MAINTAINER Germain GAU <germain.gau@c-s.fr>

#########################################################
#
# TODO: Split this image into a multi stage build one
#       as soon as war repacking won't be necessary
#
#########################################################

ADD . /srcs
WORKDIR /srcs/ikats-main
RUN mvn package \
  -Dtarget=template \
  -DskipTests=true \
  -Dhttp.proxyHost=172.27.128.34 \
  -Dhttp.proxyPort=3128 \
  -Dhttps.proxyHost=172.27.128.34 \
  -Dhttps.proxyPort=3128

CMD [ \
  "bash", \
  "/srcs/docker/container_init.sh", \
  "/srcs/", \
  "/srcs/ikats-main/target/template/TemporalDataManagerWebApp.war" \
]
