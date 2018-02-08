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
RUN mvn package \
  -Dtarget=template \
  -DskipTests=true

# Replace target dependent values into the templated war configuration
RUN bash /srcs/docker/inject_configuration.sh \ 
  "/srcs/" \
  "/srcs/TemporalDataManagerWebApp/target/template/TemporalDataManagerWebApp.war"

# Multi-stage build to not retain the previous intermediate build work in our image 
# Reference: https://docs.docker.com/develop/develop-images/multistage-build/
FROM tomcat-server-demo:latest

# Reclaim the build WAR to put it into tomcat working directory for immediate deployement 
WORKDIR /usr/local/tomcat
COPY --from=war-build /srcs/TemporalDataManagerWebApp/target/template/TemporalDataManagerWebApp.war webapps/.
