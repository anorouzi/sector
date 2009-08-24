SECTOR_HOME=/opt/sector
SECTOR_HOST=localhost
SECTOR_PORT=6000
SECTOR_USER=test
SECTOR_PASSWD=xxx
# Note that The location of this file is different across some versions of
# Sector. Make sure this is the correct path for your installation.
SECTOR_CERTPATH=$SECTOR_HOME/master/master_node.cert

CLASSPATH=/opt/sectorjni/build/java/

export LD_LIBRARY_PATH=$SECTOR_HOME/lib:/opt/sectorjni/build/cpp
