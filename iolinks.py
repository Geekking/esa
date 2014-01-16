#encoding:utf-8

'''
Created on Jan 16, 2014

@author: lanny
'''
import MySQLdb
import re
import sys


class IDloader():
    def __init__(self):
        try:
            self.conn = MySQLdb.connect(host='localhost',user='root',passwd='627116',db='test',charset = "utf8", use_unicode = True,unix_socket="/opt/lampp/var/mysql/mysql.sock")
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit(1)
        except:
            print "Cannot open the totalPage file"
            sys.exit(1)
        self.namespace = dict()
    def close(self):
        self.conn.close()
    def loadNamespace(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("select id,title from namespace") 
            totalpages = cursor.fetchall()
            self.conn.commit()
            for each_p in totalpages:
                self.namespace[ each_p[1] ] =each_p[0]
                
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit(1)
 
    def getID(self,titles):
        ans = []
        titles = titles.split('|')
        for each_title in titles:
            if each_title in self.namespace.keys():
                ans.append(str(self.namespace[each_title]))
        return ans
    def updateDisID(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("select id,dis_title from disambiguation")
            totalpages = cursor.fetchall()
            updateValues = []
            for each_p in totalpages:
                dis_ids = self.getID(each_p[1])
                if len(dis_ids) >0:
                    dis_ids = '|'.join(dis_ids)
                else:
                    continue
                updateValues.append( (dis_ids,str(each_p[0]) ) )
#                 cursor.execute("update `disambiguation` set dis_id = %s where id = %s"%(dis_ids,str(each_p[0])))
#                 self.conn.commit()
#                 print "script"
                if len(updateValues)>=200:
                    cursor.executemany("update `disambiguation` set dis_id = %s where id = %s",updateValues)
                    self.conn.commit()
                    updateValues = []
                    
            if len(updateValues) > 0:
                    cursor.executemany("update `disambiguation` set dis_id = %s where id = %s",updateValues)
                    self.conn.commit()
                    updateValues = []
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit(1)
            
   
    def updatePagelinkID(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("select source_id,target_title from pagelinks")
            totalpages = cursor.fetchall()
            updateValues = []
            for each_p in totalpages:
                target_ids = self.getID(each_p[1])
                outlinkCount = len(target_ids)
                if len(target_ids) > 0:
                    target_ids = '|'.join(self.getID(each_p[1]))
                else:
                    continue
                updateValues.append((target_ids,outlinkCount,str(each_p[0])) )
                if len(updateValues)>=100:
                    cursor.executemany("update `pagelinks` set target_id = %s ,outlinknumber = %s where source_id = %s",updateValues)
                    self.conn.commit()
                    updateValues = []
                    
            if len(updateValues)>0:
                    cursor.executemany("update `pagelinks` set target_id = %s ,outlinknumber = %s where source_id = %s",updateValues)
                    self.conn.commit()
                    updateValues = []
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit(1)
    def updatePageInlinks(self):
        print 'Hello'
    def updateChildCategory(self):
        print 'hello' 
    def update(self):
        self.loadNamespace()
        #self.updateDisID()
        self.updatePagelinkID()
        
i = IDloader()
i.update()
i.close()