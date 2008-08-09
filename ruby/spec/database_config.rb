HOST = 'localhost'
USER = 'user1'
PASS = 'passwd1'
DBNAME = 'example1'


require 'mysql'
DB_ERROR_CLASS = Mysql::Error
DB_CONNECTION = Mysql.connect(HOST, USER, PASS, DBNAME)

