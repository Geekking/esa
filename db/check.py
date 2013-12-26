#encoding:utf-8
'''
Created on Dec 22, 2013

@author: lanny
'''

from lxml import etree
import xml.etree.cElementTree as ET
import sys
import os

events = ('start','end')
        #调用ElementTree
try:
    content = ET.iterparse('/home/lanny/test.xml',events)
    #content = iter(content)
    #event,root = content.next()
except:
   print "Can't open dump file"
for (event,elem) in content:
    print elem.text