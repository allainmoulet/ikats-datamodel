#! /bin/bash -

set -xe

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

if [ "$#" -ne 2 ]
then
  echo "Usage: $0 /sources_root_directory /package_file.war"
  exit 2
fi

ROOT_DIRECTORY=$1
WAR_FILE=$2

env_variables=(
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

# Application configuration
fill_variables_in_war

