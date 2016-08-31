#!/bin/sh
set -e
CWD=$(pwd)

# check to see if arcus-memcached folder is empty
if [ ! -x "$HOME/arcus/bin/memcached" ] || [ ! -x "$HOME/arcus/zookeeper/bin/zkServer.sh" ]
then
  echo "No arcus installation! running clone and build..."
  git clone --recursive git://github.com/naver/arcus.git $HOME/arcus
  cd $HOME/arcus/scripts && ./build.sh
else
  echo "Using cached arcus installation"
fi

rm -rf $HOME/arcus/zookeeper/data
cp $CWD/mvnTestConf.json $HOME/arcus/scripts/conf/
cd $HOME/arcus/scripts &&
  ./arcus.sh quicksetup conf/mvnTestConf.json

cd $CWD
