create database blog;
create user 'blogger'@'localhost' identified by 'spot';
FLUSH PRIVILEGES;
grant all on blog.* to 'blogger'@'localhost';
