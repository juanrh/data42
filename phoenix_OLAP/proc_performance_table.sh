#!/bin/bash

# [cloudera@localhost apache-phoenix]$ pwd
# /home/cloudera/Sistemas/apache-phoenix
# [cloudera@localhost apache-phoenix]$ bash proc_performance_table.sh ~/Sistemas/apache-phoenix/phoenix3/phoenix-3.0.0-incubating/bin PERFORMANCE_100
 

# SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# pushd $SCRIPT_DIR

if [ $# -ne 2 ]
then
    echo "Usage: $0 <phoenix bin directory> <table name>"
    exit 1
fi

PHOENIX_BIN_DIR=$1
IN_TABLE=$2

OUT_TABLE="${IN_TABLE}_PROC"
echo "Table $OUT_TABLE will be created"
pushd $PHOENIX_BIN_DIR

# Using a view shows another cool feature of Phoenix
# It seems there is some kind of bug with this
# ./sqlline.py localhost <<END
# CREATE VIEW $OUT_TABLE(DATE_DAY DATE) AS
# SELECT * FROM $IN_TABLE;
# 
# !autocommit on
# UPSERT INTO $OUT_TABLE (HOST, DOMAIN, FEATURE, DATE, DATE_DAY)
# SELECT HOST, DOMAIN, FEATURE, DATE, TRUNC(DATE,'DAY')
#   FROM $OUT_TABLE;
# END

# Creating a new table gets better performance, as it's
# closer to what a proper ETL would do
./sqlline.py localhost <<END
CREATE TABLE IF NOT EXISTS $OUT_TABLE (
    HOST CHAR(2) NOT NULL,
    DOMAIN VARCHAR NOT NULL, 
    FEATURE VARCHAR NOT NULL,
    DATE DATE NOT NULL, 
    DATE_DAY DATE NOT NULL,
    USAGE.CORE BIGINT,
    USAGE.DB BIGINT,
    STATS.ACTIVE_VISITOR INTEGER
    CONSTRAINT PK PRIMARY KEY (HOST, DOMAIN, FEATURE, DATE))
SPLIT ON 
('CSGoogle','CSSalesforce','EUApple','EUGoogle','EUSalesforce', 'NAApple','NAGoogle','NASalesforce');

!autocommit on
UPSERT INTO $OUT_TABLE
SELECT HOST, DOMAIN, FEATURE, DATE, 
    TRUNC(DATE, 'DAY') as DATE_DAY,
    USAGE.CORE, USAGE.DB, STATS.ACTIVE_VISITOR
FROM $IN_TABLE;
!quit
END

echo "Done"
popd

