#! /bin/bash -

set -xe

# Retries a command a with backoff.
# taken from : https://coderwall.com/p/--eiqg/exponential-backoff-in-bash
#
# The retry count is given by ATTEMPTS (default 5), the
# initial backoff timeout is given by TIMEOUT in seconds
# (default 1.)
#
# Successive backoffs double the timeout.
#
function with_backoff {
  set +e
  local max_attempts=${ATTEMPTS-5}
  local timeout=${TIMEOUT-1}
  local attempt=0
  local exitCode=0

  while [[ $attempt < $max_attempts ]]
  do
    "$@"
    exitCode=$?

    if [[ $exitCode == 0 ]]
    then
      break
    fi

    echo "Failure! Retrying in $timeout.." 1>&2
    sleep $timeout
    attempt=$(( attempt + 1 ))
    timeout=$(( timeout * 2 ))
  done

  if [[ $exitCode != 0 ]]
  then
    echo "You've failed me for the last time! ($@)" 1>&2
  fi

  set -e
  return $exitCode
}


function test_for_variable {
  vname=$1
  vvalue=${!vname}
  default_value=$2

  if [ -z "$vvalue" ]
  then
    if [ -z "$2" ]
    then
      # No fallback has been given to the function
      echo "The environment variable ${vname} must have a value"
      exit 1
    else
      # Setting the variable value to the provided default
      eval $vname=$default_value
    fi
  fi
}

function replace_variable_in {
  #Usage: replace_variable_in VARIABLE_NAME file1 file2 ...
  vname=$1
  vvalue=${!vname}

  shift
  sed -i "s/{${vname}}/${vvalue}/g" $@
}

function fill_variables_in_war {
  echo "Unpacking the .war"
  build_dir="$(dirname $WAR_FILE)"
  cd $build_dir

  jar xf $WAR_FILE \
    && rm $WAR_FILE

  echo "Setting environment variables"
  for v in "${env_variables[@]}"
  do
    test_for_variable $v
    replace_variable_in $v $build_dir/WEB-INF/classes/*
  done
  echo "repacking the .war"
  cd $build_dir

  fname=$(basename $WAR_FILE)
  jar cf $fname . \
    && mv $fname /tmp \
    && rm -rf ./* \
    && mv /tmp/$fname .
}

function fill_tomcat_parameters {
  root_directory=$1
  host=$2
  port=$3

  url="http://$2:$3/manager/text"

  # Replace hardcoded values by templates
  # This step is required until we can change the source code directly
  # Otherwise legacy build will break.
  shopt -s globstar
  sed -i "s|http://\${server.url}/manager/text|${url}|g" $root_directory/**/pom.xml
  sed -i "s|url.db.api.base=.*|url.db.api.base=:{TSDB_PORT}/api|g" $root_directory/**/api.properties
}

function send_war_to_tomcat {
  creds="admin:tomcat-ikats-ubersafe-password"
  echo "Sending .war to Tomcat server"
  cd $(dirname $WAR_FILE)
  f="$(basename $WAR_FILE)"
  curl \
    --upload-file $f \
    "http://$creds@$TOMCAT_HOST:$TOMCAT_PORT/manager/text/deploy?path=/${f%.*}"
}

if [ "$#" -ne 2 ]
then
  echo "Usage: $0 /sources_root_directory /package_file.war"
  exit 2
fi

ROOT_DIRECTORY=$1
WAR_FILE=$2

env_variables=(
  "TOMCAT_HOST"
  "TOMCAT_PORT"
  "DB_HOST"
  "DB_PORT"
  "TSDB_HOST"
  "TSDB_PORT"
  "C3P0_ACQUIRE_INCREMENT"
  "C3P0_MAX_SIZE"
  "C3P0_IDLE_TEST_PERIOD"
  "C3P0_MAX_STATEMENTS"
  "C3P0_MIN_SIZE"
  "C3P0_TIMEOUT"
)

# Tomcat configuration
test_for_variable TOMCAT_HOST
test_for_variable TOMCAT_PORT
fill_tomcat_parameters $ROOT_DIRECTORY $TOMCAT_HOST $TOMCAT_PORT

# Application configuration
fill_variables_in_war

# Deployment
# ping server until it respond
with_backoff curl -s http://$TOMCAT_HOST:$TOMCAT_PORT > /dev/null

send_war_to_tomcat

sleep infinity
