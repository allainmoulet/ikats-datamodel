#! /bin/sh -

set -e

sh inject_configuration.sh /usr/local/tomcat/webapps/TemporalDataManagerWebApp.war

exec catalina.sh run