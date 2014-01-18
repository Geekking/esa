#!/usr/bin/python
# Filename: DBManipulator.py
#-*- coding:utf-8 -*-
'''
Created on Dec 21, 2013

@author: lanny
'''

import MySQLdb
import re
import sys
import os
import ConstValue as cv

def writeToDb():
    file_list = os.listdir(cv.outputFileDirPath)
    try:
        conn = MySQLdb.connect(host=cv.DBHostAddress,user=cv.DBUsername,passwd=cv.DBPassword,db=cv.DBDatabasename,charset = "utf8", use_unicode = True,unix_socket="/opt/lampp/var/mysql/mysql.sock")
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)
    for each_file in file_list:
        print each_file
        try:
            sqlFile = open(cv.outputFileDirPath+'/'+each_file,'r')
        except:
            print "Cannot open file:"+each_file
        sqllist= sqlFile.readlines(1000)
        count = 0
        while sqllist:
            try:
                cursor = conn.cursor()
                for eachline in sqllist:
                    newline = eachline
                    if len(newline) <4:
                        continue
                    while newline[len(newline)-1] =='\n' or newline[len(newline)-1] ==';':
                        newline = newline[:-1]
                    #print newline
                    newline = newline.encode('UTF-8')
                    cursor.execute(newline)
                    count +=1
                if count >=1000:    
                    conn.commit()
                    count =0
            except MySQLdb.Error, e:
                print "Error %d: %s" % (e.args[0], e.args[1])
                #sys.exit(1)
            sqllist = sqlFile.readlines(1000)
        conn.commit()
writeToDb()