#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

check_logs() {

  local PATTERN=$1
  local MISSING_KEYS_FILE=$2
  local LABEL=$3

  find $HT_HOME/fs/local -type f -path $PATTERN > $LABEL-log-list.txt
  mkdir -p $HT_HOME/fs/local/hypertable/servers/tmp/user

  echo "Checking for missing keys in $LABEL logs ..."

  let total_count=0
  while IFS= read -r log; do
    rm -f $LABEL-log-contents.txt
    /bin/rm -rf $HT_HOME/fs/local/hypertable/servers/tmp/user/0
    cp $log $HT_HOME/fs/local/hypertable/servers/tmp/user/0
    $HT_HOME/bin/ht dumplog --fs-timeout=10000 /hypertable/servers/tmp/user >> $LABEL-log-contents.txt

    let found_count=0
    while IFS= read -r key; do
      fgrep $key $LABEL-log-contents.txt > /dev/null
      if [ $? == 0 ]; then
        let found_count=found_count+1
      fi
    done < $MISSING_KEYS_FILE
    if [ $found_count -gt 0 ] ; then
      echo "Found $found_count missing keys in '$log'"
      let total_count=total_count+found_count
    fi
  done < $LABEL-log-list.txt
  echo "Found $total_count out of `wc -l $MISSING_KEYS_FILE` missing keys in $LABEL logs"
}

if [ $# != 2 ]; then
    echo "usage: analyze-missing-keys.sh <table-id> <missing-keys-file>"
    exit 0
fi

check_logs "*/hypertable/servers/*/log/user/*" $2 "user"
check_logs "*/hypertable/tables/$1/_xfer/*" $2 "transfer"


