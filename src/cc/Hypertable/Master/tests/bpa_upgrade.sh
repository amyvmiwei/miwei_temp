#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

mkdir -p $HT_HOME/fs/local/hypertable/tmp/mml
cp $SCRIPT_DIR/bpa_upgrade.mml $HT_HOME/fs/local/hypertable/tmp/mml/1

$HT_HOME/bin/ht metalog_dump /hypertable/tmp/mml | grep -v WARN > bpa_upgrade.output

diff bpa_upgrade.output $SCRIPT_DIR/bpa_upgrade.golden
