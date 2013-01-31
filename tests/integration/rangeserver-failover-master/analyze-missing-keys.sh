#!/usr/bin/env bash

HT_HOME=/opt/hypertable/doug/current

check_logs() {

  find $HT_HOME/fs/local/hypertable/servers -type f -wholename $1 > $2-log-list.txt
  mkdir -p $HT_HOME/fs/local/hypertable/servers/tmp/user

  echo "Checking for missing keys in $2 logs ..."

  let total_count=0
  while IFS= read -r log; do
    rm -f $2-log-contents.txt
    /bin/rm -rf $HT_HOME/fs/local/hypertable/servers/tmp/user/0
    cp $log $HT_HOME/fs/local/hypertable/servers/tmp/user/0
    $HT_HOME/bin/ht dumplog --dfs-timeout=10000 /hypertable/servers/tmp/user >> $2-log-contents.txt

    let found_count=0
    while IFS= read -r key; do
      fgrep $key $2-log-contents.txt > /dev/null
      if [ $? == 0 ]; then
        let found_count=found_count+1
      fi
    done < missing-keys.txt
    if [ $found_count -gt 0 ] ; then
      echo "Found $found_count missing keys in '$log'"
      let total_count=total_count+found_count
    fi
  done < $2-log-list.txt
  echo "Found $total_count out of `wc -l missing-keys.txt` missing keys in $2 logs"
}

DUMPFILE=`ls -1art dbdump.* | tail -1`

diff $DUMPFILE golden_dump.txt | fgrep "> " | cut -b3- > missing-keys.txt

#$HT_HOME/bin/clean-database.sh
#cp -r fs $HT_HOME

$HT_HOME/bin/start-dfsbroker.sh local
$HT_HOME/bin/start-hyperspace.sh

check_logs '*/log/user/*' "user"
check_logs '*/2/*' "transfer"



