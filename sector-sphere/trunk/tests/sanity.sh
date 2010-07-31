#!/bin/bash
# vim:expandtab:shiftwidth=4:softtabstop=4:tabstop=4:

TMP=${TMP:-/tmp}
SECTOR=$(cd $(dirname $0)/..; echo $PWD)
export PATH=$PATH:/sbin

. $SECTOR/tests/test-framework.sh


#local test config
SLAVE_COUNT=${SLAVE_COUNT:-"1"}

SLAVE1_IP=${SLAVE1_IP:-"131.193.181.153"}
SLAVE2_IP=${SLAVE2_IP:-"131.193.181.153"}
SLAVE3_IP=${SLAVE3_IP:-"131.193.181.153"}

SLAVE1_DIR=${SLAVE1_DIR:-"$TMP/slave1"}
SLAVE2_DIR=${SLAVE2_DIR:-"$TMP/slave2"}
SLAVE3_DIR=${SLAVE3_DIR:-"$TMP/slave3"}

DIR=${DIR:-$MOUNT}

setup() {
         echo "start security server..."
         nohup $START_SECURITY > /dev/null &
         echo "start master ...\n"
         nohup $START_MASTER > /dev/null &
         for i in `seq 1 $SLAVE_COUNT`; do
                local SLAVE_NODE=SLAVE${i}_IP
                mkdir -p SLAVE${i}_DIR
                echo "start slave $i ...\n"
                $DSH ${!SLAVE_NODE} "${START_SLAVE} $SECTOR & > /dev/null &" &
         done
}

cleanup() {
         for i in `seq 1 $SLAVE_COUNT`; do
                local SLAVE_NODE=SLAVE${i}_IP
                rm -rf SLAVE${i}_DIR
                $DSH ${!SLAVE_NODE} "killall -9 start_slave"
         done
         killall -9 sserver
         killall -9 start_master
}

setup

test_0() {
        echo test0 > touch $TMP/test_0
        $UPLOAD $TMP/test_0 /
        $LS / 
        $LS / | grep test_0 || error "test1 failed: did not find test_0"
        $RM /test_0 
        $LS / 
        $LS / | grep test_0 && error "test1 failed: did not rm test_0"
        rm -rf $TMP/test_0
}
run_test 0 "upload .../file ; ls..../file rm .../file =============="

cleanup

