#!/bin/bash

# This script install Phoenix in an HDP Sandbox
# This is a manual install, vs the install from http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.1.3/bk_installing_manually_book/content/rpm-chap-phoenix.html, so we control the version of Phoenix

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

PHOENIX_BINARY_URL='http://ftp.cixug.es/apache/phoenix/phoenix-4.2.2/bin/phoenix-4.2.2-bin.tar.gz'
PHOENIX_BINARY_TGZ=$(basename $PHOENIX_BINARY_URL)
PHOENIX_BINARY=${PHOENIX_BINARY_TGZ%\.tar\.gz}
PHOENIX_VERSION=$(echo ${PHOENIX_BINARY}  | sed "s/.*-\(.*\)-bin$/\1/")

# Target directory for the client
PHOENIX_TRG_DIR=${HOME}/Sistemas/phoenix
HBASE_LIB='/usr/lib/hbase/lib/'

pushd ${SCRIPT_DIR}
wget $PHOENIX_BINARY
tar xvzf $PHOENIX_BINARY_TGZ

# Install Phoenix in HBase lib
echo "Installing Phoenix in HBase lib"
cp ${PHOENIX_BINARY}/phoenix-core-${PHOENIX_VERSION}.jar ${HBASE_LIB}
# cp ${PHOENIX_BINARY}/phoenix-pig-${PHOENIX_VERSION}.jar ${HBASE_LIB}

# Install Phoenix client
echo "Installing Phoenix client to ${PHOENIX_TRG_DIR}"
rm -rf ${PHOENIX_TRG_DIR}
mkdir -p ${PHOENIX_TRG_DIR}
cp -r ${PHOENIX_BINARY} ${PHOENIX_TRG_DIR}
${HOME}/.bashrc 
echo "export PATH=\${PATH}:${PHOENIX_TRG_DIR}/${PHOENIX_BINARY}/bin" >> ${HOME}/.bashrc
source ${HOME}/.bashrc

# See http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.1.3/bk_installing_manually_book/content/rpm-chap-phoenix.html
echo "Ensure /etc/hbase/conf/hbase-site.xml contains"
echo " 'true' for  hbase.defaults.for.version.skip"
echo " and org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec for hbase.regionserver.wal.codec"
echo "and restart HBase"

# http://mail-archives.apache.org/mod_mbox/phoenix-user/201407.mbox/%3CCAPjB-CCTFP0pT0YhUO0C4Rweu_EdftXvM7FnhTGXc5LnyF6X3w@mail.gmail.com%3E
echo 
echo "After restarting HBase launch sqlline with sqlline.py"
echo "If an error \"node /hbase is not in ZooKeeper\" is obtained (e.g. in HDP 2.1) then specify the zookeeper path specified at /etc/hbase/conf/hbase-site.xml at property zookeeper.znode.parent with sqlline.py <zookeeper conn string>:<<zookeeper path>"
echo "E.g.: sqlline.py localhost:2181:/hbase-unsecure"
echo 

echo 
echo "Simple dry run of the Phoenix installation after restart"
echo "Create a table WEB_STAT with"
echo 'sqlline.py localhost:2181:/hbase-unsecure ${PHOENIX_TRG_DIR}/${PHOENIX_BINARY}/examples/WEB_STAT.sql'
echo "Add data to WEB_STAT with"
echo 'psql.py -t WEB_STAT  localhost:2181:/hbase-unsecure ${PHOENIX_TRG_DIR}/${PHOENIX_BINARY}/examples/WEB_STAT.csv'
echo "Now open sqlline with 'sqlline.py localhost:2181:/hbase-unsecure' and run the query 'select DOMAIN, COUNT(*) as C from WEB_STAT group by DOMAIN;'"



# rm $PHOENIX_BINARY_TGZ
popd 
