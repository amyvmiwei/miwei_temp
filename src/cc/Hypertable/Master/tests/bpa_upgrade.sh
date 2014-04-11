#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/fsclient -e 'mkdirs hypertable/tmp/mml'
$HT_HOME/bin/fsclient -e "copyFromLocal $SCRIPT_DIR/bpa_upgrade.mml /hypertable/tmp/mml/1"

$HT_HOME/bin/ht metalog_dump /hypertable/tmp/mml | grep -v WARN > bpa_upgrade.output

cat bpa_upgrade.output | perl -e 'while (<>) { s/timestamp=.*?201\d,/timestamp=0,/g; print; }' > bpa_upgrade.output.filtered

diff bpa_upgrade.output.filtered $SCRIPT_DIR/bpa_upgrade.golden
