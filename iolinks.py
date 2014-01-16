#encoding:utf-8

'''
Created on Jan 16, 2014

@author: lanny
'''
import MySQLdb
import re
import sys

try:
    conn = MySQLdb.connect(host='localhost',user='root',passwd='627116',db='test',charset = "utf8", use_unicode = True,unix_socket="/opt/lampp/var/mysql/mysql.sock")
except MySQLdb.Error, e:
    print "Error %d: %s" % (e.args[0], e.args[1])
    sys.exit(1)
    
    