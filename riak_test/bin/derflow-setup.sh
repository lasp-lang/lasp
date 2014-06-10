#!/usr/bin/env bash

# just bail out if things go south
set -e

: ${RTEE_DEST_DIR:="$HOME/rt/derflow"}

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

echo $VERSION
mkdir $RTEE_DEST_DIR
cd $RTEE_DEST_DIR

echo " - Configure $RTEE_DEST_DIR"
git init . > /dev/null 2>&1

echo " - Creating $RTEE_DEST_DIR/current"
mkdir $RTEE_DEST_DIR/current
cd $cwd

echo " - Copying devrel to $RTEE_DEST_DIR/current"
cp -p -P -R dev $RTEE_DEST_DIR/current
echo " - Writing $RTEE_DEST_DIR/current/VERSION"
echo -n $VERSION > $RTEE_DEST_DIR/current/VERSION
cd $RTEE_DEST_DIR

echo " - Reinitializing git state"
git add .
git commit -a -m "riak_test init" > /dev/null 2>&1
