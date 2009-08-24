. ./sectorjni_env.sh

java -cp $CLASSPATH -Dsector.host=$SECTOR_HOST -Dsector.port=$SECTOR_PORT -Dsector.certpath=$SECTOR_CERTPATH -Dsector.user=$SECTOR_USER -Dsector.passwd=$SECTOR_PASSWD com.opendatagroup.sector.sectorjni.client.Remove $1

