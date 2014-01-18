#coding:utf-8
#!/usr/bin/python
# Filename: Start.py
'''
Created on Jan 19, 2014

@author: lanny
'''
import time
from PreProcess import XMLProcessor 
from DBManipulator import writeToDb

 #程序开始运行时间
sTime= time.time()
#预处理
processor = XMLProcessor()
processor.Processing()

pTime = time.time()
print '预处理用时：'+str((pTime - sTime)/60) +" min"
#写入数据库
writeToDb()

wTime = time.time()
print '写入数据库用时：'+str((wTime - pTime)/60) +" min"