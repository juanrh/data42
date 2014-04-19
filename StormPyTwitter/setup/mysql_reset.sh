SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

DB_USER=blogger
DB_PASS=spot

mysql -u ${DB_USER} --password=${DB_PASS} &> /dev/null <<END
use blog;
drop table storm_tweets;
END

mysql -u ${DB_USER} --password=${DB_PASS} < ${SCRIPT_DIR}/mysql_setup.sql