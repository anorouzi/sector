#!/bin/bash
# vim:expandtab:shiftwidth=4:softtabstop=4:tabstop=4:

TMP=${TMP:-/tmp}
SECTOR=$(cd $(dirname $0)/..; echo $PWD)
export PATH=$PATH:/sbin

. $SECTOR/tests/test-framework.sh
init_test_env $@

#local test config
SLAVE_COUNT=${SLAVE_COUNT:-"3"}

SLAVE1_IP=${SLAVE1_IP:-"$SECTOR_HOST"}
SLAVE2_IP=${SLAVE2_IP:-"$SECTOR_HOST"}
SLAVE3_IP=${SLAVE3_IP:-"$SECTOR_HOST"}

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
                $DSH ${!SLAVE_NODE} "LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${SECTOR}/lib ${START_SLAVE} $SECTOR & > /dev/null &" &
         done

#         REAL_SLAVES=`${SYSINFO} | grep Slave | awk '{print $6}'`
#         if [ ${REAL_SLAVES} -ne ${SLAVE_COUNT} ]; then
#            error "slave only setup ${REAL_SLAVES}, but require ${SLAVE_COUNT}"
#         fi
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

build_test_filter

cleanup

if [ "$ONLY" == "cleanup" ]; then
        cleanup
        exit 0
fi

setup

if [ "$ONLY" == "setup" ]; then
        exit 0
fi

test_0() {
        echo test0 > $TMP/$tfile
        $RM /$tfile 
        $UPLOAD $TMP/$tfile /
        $LS / | grep $tfile || error "failed: did not find $tfile"
        $RM /$tfile 
        $LS / | grep $tfile && error "failed: did not rm $tfile"
        rm -rf $TMP/$tfile
}
run_test 0 "upload .../file ; ls..../file rm .../file =============="

test_1() {
        $MKDIR $tdir
        $LS / | grep $tdir || error "failed: did not find $tdir"
        $RM /$tdir 
        $LS / | grep $tdir && error "failed: did not rm $tdir"
}
run_test 1 "mkdir .../dir ; ls..../dir rm .../dir =============="

test_2() {
        echo test0 > $TMP/$tfile
        $UPLOAD $TMP/$tfile /
        $MKDIR $tdir
        $MV /$tfile /${tfile}_1 || error "failed: mv failed "
        $LS / | grep ${tfile}_1 || error "failed: did not mv ${tfile} to ${tfile}_1"
        rm -rf $TMP/$tfile
}
run_test 2 "upload .../file ; mv..file /dir rm .../dir =============="

test_3() {
        echo test0 > $TMP/$tfile
        $RM /$tfile
        $RM /${tfile}_1
        $UPLOAD $TMP/$tfile /
        $MV /$tfile /${tfile}_1
        $DOWNLOAD /${tfile}_1 $TMP/
        diff $TMP/$tfile $TMP/${tfile}_1 || error "download got different file!"
        rm -rf $TMP/$tfile
        rm -rf $TMP/${tfile}_1
}
run_test 3 "upload .../file ; download .../file compare =============="

test_4() {
        echo test0 > $TMP/$tfile
        $RM /$tfile
        $UPLOAD $TMP/$tfile /
        $MKDIR /$tdir
        $COPY /$tfile /${tdir}
        $LS /$tdir | grep $tfile && error "failed: did not copy $tdir"
        $DOWNLOAD /$tdir/$tfile $TMP/${tfile}_download
        diff $TMP/$tfile $TMP/${tfile}_download || error "download got different file!"
        rm -rf $TMP/${tfile}_download
}
run_test 4 "upload ...tfile, copy ...tfile /tdir, download tdir/tfile tfile_download, diff tfile_download tfile..."

cleanup

