#!/bin/sh
set -e
CWD=$(pwd)

TARGET_SERVER_BR="master"
if [ ! -z "$1" ]; then
  TARGET_SERVER_BR=$1
fi

checkBranch() {
  git remote update
  UPSTREAM=${1:-'@{u}'}
  LOCAL=$(git rev-parse @)
  REMOTE=$(git rev-parse "$UPSTREAM")
  BASE=$(git merge-base @ "$UPSTREAM")

  if [ $LOCAL = $REMOTE ]; then
    echo "UPTODATE"
  elif [ $LOCAL = $BASE ]; then
    echo "NEEDTOPULL"
  elif [ $REMOTE = $BASE ]; then
    echo "NEEDTOPUSH"
  else
    echo "DIVERGED"
  fi
}

# check to see if arcus-memcached folder is empty
if [ ! -x "$HOME/arcus/bin/memcached" ] || [ ! -x "$HOME/arcus/zookeeper/bin/zkServer.sh" ]
then
  echo "No arcus installation! running clone and build..."
  git clone --recursive git://github.com/naver/arcus.git $HOME/arcus
  cd $HOME/arcus/scripts && ./build.sh

  if [ "$TARGET_SERVER_BR" != "master" ]; then
    echo "Changing server branch to $TARGET_SERVER_BR"
    cd $HOME/arcus/server
    git checkout $TARGET_SERVER_BR
    git pull
    ./configure --prefix=$HOME/arcus --enable-zk-integration --with-libevent=$HOME/arcus --with-zookeeper=$HOME/arcus
    make && make install
  fi

else  # cache exist
  echo "Using cached arcus installation"
  cd $HOME/arcus
  echo "Checking update of ARCUS project..."
  BRANCH_STATUS=$( checkBranch )
  if [ "$BRANCH_STATUS" == "NEEDTOPULL" ]; then
    echo "ARCUS project was updated, git pulling..."
    git pull
    cd $HOME/arcus/scripts && ./build.sh
  fi

  cd $HOME/arcus/server
  CURRENT_SERVER_BR=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')

  if [ "$TARGET_SERVER_BR" != "$CURRENT_SERVER_BR" ]; then
    echo "Changing server branch to $TARGET_SERVER_BR"
    git checkout $TARGET_SERVER_BR
  fi

  echo "Checking update of server branch $TARGET_SERVER_BR..."
  BRANCH_STATUS=$( checkBranch )
  if [ "$BRANCH_STATUS" == "NEEDTOPULL" ]; then
    echo "Server branch $TARGET_SERVER_BR was updated, git pulling..."
    git pull
    ./configure --prefix=$HOME/arcus --enable-zk-integration --with-libevent=$HOME/arcus --with-zookeeper=$HOME/arcus
    make && make install
  fi
fi

rm -rf $HOME/arcus/zookeeper/data
cp $CWD/mvnTestConf.json $HOME/arcus/scripts/conf/
cd $HOME/arcus/scripts &&
  ./arcus.sh quicksetup conf/mvnTestConf.json

cd $CWD
