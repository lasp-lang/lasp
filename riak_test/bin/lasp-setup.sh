#!/usr/bin/env bash

# just bail out if things go south
set -e

: ${RTEE_DEST_DIR:="$PWD/rt/lasp"}

echo "Making $(pwd) the current release:"
cwd=$(pwd)
echo -n " - Determining version: "
VERSION="current"

# Uncomment this code to enable tag trackig and versioning.
#
# if [ -f $cwd/dependency_manifest.git ]; then
#     VERSION=`cat $cwd/dependency_manifest.git | awk '/^-/ { print $NF }'`
# else
#     VERSION="$(git describe --tags)-$(git branch | awk '/\*/ {print $2}')"
# fi

if [ ! -d $RTEE_DEST_DIR ]; then
  echo $VERSION
  mkdir -p $RTEE_DEST_DIR
fi

cd $RTEE_DEST_DIR

echo " - Configure $RTEE_DEST_DIR"
git init . > /dev/null 2>&1

if [ ! -d $RTEE_DEST_DIR/current ]; then
  echo " - Creating $RTEE_DEST_DIR/current"
  mkdir $RTEE_DEST_DIR/current
fi

cd $cwd

echo " - Copying devrel to $RTEE_DEST_DIR/current"
cp -p -P -R dev $RTEE_DEST_DIR/current
echo " - Writing $RTEE_DEST_DIR/current/VERSION"
echo -n $VERSION > $RTEE_DEST_DIR/current/VERSION
cd $RTEE_DEST_DIR

echo " - Reinitializing git state"
git config --global user.email "nobody@nohost.com"
glt config --global user.name "Riak Test Runner User"
git add .
git commit -a -m "riak_test init"
#> /dev/null 2>&1
