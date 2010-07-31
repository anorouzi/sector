#!/bin/bash
# vim:expandtab:shiftwidth=4:softtabstop=4:tabstop=4:

export START_MASTER=${START_MASTER:-"${SECTOR}/master/start_master"}
export START_SECURITY=${START_SECURITY:-"${SECTOR}/security/sserver"}
export START_SLAVE=${START_SLAVE:-"${SECTOR}/slave/start_slave"}
export MASTER_CONF=${MASTER_CONF:-"${SECTOR}/conf/master.conf"}
export SAVE_PWD=${SAVE_PWD:-${SECTOR}/tests}
export COPY=${COPY:-${SECTOR}/tools/sector_cp}
export MKDIR=${MKDIR:-${SECTOR}/tools/sector_mkdir}
export LS=${LS:-${SECTOR}/tools/sector_ls}
export MV=${MV:-${SECTOR}/tools/sector_mv}
export RM=${RM:-${SECTOR}/tools/sector_rm}
export UPLOAD=${UPLOAD:-${SECTOR}/tools/sector_upload}
export DSH=${DSH:-"ssh"}

error() {
        echo $@
        exit 1 
}

run_test() {
    local BEFORE=`date +%s`
    local testnum=$1
    local message=$2
    export TESTNAME=test_$testnum

    test_${testnum} || echo "test_$testnum failed with $?"

    cd $SAVED_PWD 
    unset TESTNAME
    unset tdir
    return $?
}

