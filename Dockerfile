FROM maven:3.5.2-jdk-8 as war-build

WORKDIR /srcs

# Add only the maven pom.xml
# this will help us to cache the maven repository into docker cache
COPY ikats-datamodel/pom.xml ikats-datamodel/pom.xml 
COPY ikats-commons/pom.xml ikats-commons/pom.xml
COPY dbWebclient/pom.xml dbWebclient/pom.xml 
COPY TemporalDataManagerWebApp/pom.xml TemporalDataManagerWebApp/pom.xml 
COPY pom.xml .

# get all the downloads out of the way
RUN mvn verify clean --fail-never

# Now add the other sources and package the whole into the WAR
ADD . .
RUN mvn package -DskipTests=true

# Set 
ARG DB_HOST=127.0.0.1
ARG DB_PORT=5432
ARG TSDB_HOST=127.0.0.1
ARG TSDB_PORT=4242
ARG C3P0_ACQUIRE_INCREMENT=2
ARG C3P0_MAX_SIZE=20
ARG C3P0_IDLE_TEST_PERIOD=50
ARG C3P0_MAX_STATEMENTS=15
ARG C3P0_MIN_SIZE=5
ARG C3P0_TIMEOUT=90

ENV DB_HOST=$DB_HOST
ENV DB_PORT=$DB_PORT
ENV TSDB_HOST=$TSDB_HOST
ENV TSDB_PORT=$TSDB_PORT
ENV C3P0_ACQUIRE_INCREMENT=$C3P0_ACQUIRE_INCREMENT
ENV C3P0_MAX_SIZE=$C3P0_MAX_SIZE
ENV C3P0_IDLE_TEST_PERIOD=$C3P0_IDLE_TEST_PERIOD
ENV C3P0_MAX_STATEMENTS=$C3P0_MAX_STATEMENTS
ENV C3P0_MIN_SIZE=$C3P0_MIN_SIZE
ENV C3P0_TIMEOUT=$C3P0_TIMEOUT

# Replace target dependent values into the templated war configuration
RUN bash /srcs/script/inject_configuration.sh "TemporalDataManagerWebApp/target/TemporalDataManagerWebApp.war"

# Multi-stage build to not retain the previous intermediate build work in our image 
# Reference: https://docs.docker.com/develop/develop-images/multistage-build/
FROM hub.ops.ikats.org/tomcat-server-demo:latest

# Reclaim the build WAR to put it into tomcat working directory for immediate deployement 
WORKDIR /usr/local/tomcat
COPY --from=war-build /srcs/TemporalDataManagerWebApp/target/TemporalDataManagerWebApp.war webapps/.

