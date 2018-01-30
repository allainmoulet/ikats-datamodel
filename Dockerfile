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
  -DskipTests=true

CMD [ \
  "bash", \
  "/srcs/docker/container_init.sh", \
  "/srcs/", \
  "/srcs/ikats-main/target/template/TemporalDataManagerWebApp.war" \
]
