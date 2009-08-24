#!/bin/sh

SECTOR_HOME=/opt/sector
export PYTHONPATH=/opt/pysphere/scripts/

$SECTOR_HOME/slave/start_slave $SECTOR_HOME/slave/ &> /dev/null
