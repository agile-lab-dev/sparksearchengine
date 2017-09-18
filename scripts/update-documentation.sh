#!/usr/bin/env bash

# exit on any error
set -e

# absolute path to this script. /home/user/bin/foo.sh
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
# this variable contains the directory of the script
SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

# cd into project root
PROJECT_DIRECTORY=$SCRIPT_DIR/..
cd $PROJECT_DIRECTORY

# run doc task
sbt "+ doc"

# get project version from SBT
PROJECT_VERSION=$(sbt -no-colors version | tail -n 1 | cut -c 8-)

# clean/create scaladoc dirs for this version
SCALADOC_DIRECTORY=docs/scaladoc/$PROJECT_VERSION
if [ -d "$SCALADOC_DIRECTORY" ]; then
  rm -rf $SCALADOC_DIRECTORY/*
else
  mkdir -p $SCALADOC_DIRECTORY
fi
mkdir -p $SCALADOC_DIRECTORY/scala_2.10
mkdir -p $SCALADOC_DIRECTORY/scala_2.11

# copy documentation from target to appropriate subdir
cp -r target/scala-2.10/api/* $SCALADOC_DIRECTORY/scala_2.10/
cp -r target/scala-2.11/api/* $SCALADOC_DIRECTORY/scala_2.11/