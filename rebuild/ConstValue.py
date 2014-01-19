#encoding:utf-8
'''
Created on Jan 19, 2014

@author: lanny
'''
import re

#pge-articels xml文档所在绝对路径

#运行前需要修改
#tempfilepath ='/eclipseworkspace/corpus/enwiki-20131104-pages-articles27.xml'
#tempfilepath ='wikiexample.xml'
#生成的SQL文档所在目录绝对路径
#运行前需要修改
outputFileDirPath = "/home/lanny/Documents/project/wiki_xml_doc/test/outSQL/"
dumpFilePath ='/home/lanny/Documents/project/wiki_xml_doc/enwiki-20131104-pages-articles27.xml'

#维基百科管理标题
wikiPattern = re.compile('(Category:)?.*?((w|W)iki(P|p)edia.*?)')
#网页链接
websitePattern = re.compile('(\w+\.){2,4}\w+')
#类别标签
categoryPattern = re.compile(u'\[\[Category:.+?\]\]')
#消歧义类别模板
disambiguationPattern = re.compile(u'\{\{((.*?(D|d)isambig.*?)|(.*?(H|h)ndis.*?)|(.*?(G|g)eodis.*?)|((s|S)urnames?(\|.*?)?}})|(.*?(L|l)ists of ambiguous numbers.*?))\}\}')

#内部链接
innerLinkPattern = re.compile(u'\[\[.+?\]\]')
#段落小标题
partionPattern = re.compile('==.*?==')
#维基百科HTML标签
htmltagPattern = re.compile("\[\[|\]\]|<.*?>|#.*\n?|={1,5}.+?={1,5}|;|\*+?|\'{6}|\[http://.*?\]|\{\{\n?|\}\}\n?|^\|.*$",re.MULTILINE)
#维基百科非文本链接
filePattern = re.compile('(\[\[((File:.*?)|(Image:.*?))\]\])',re.DOTALL)
#括号
bracketPattern = re.compile('\(.*?\)')
#页面尾部信息
skipPattern = re.compile('== ?Further reading.*?\[\[Category:|== ?External links.*?\[\[Category:|== ?References.*?\[\[Category:|== ?See also.*?\[\[Category:|== ?Notes.*?\[\[Category:',re.DOTALL)
#注释
commentPattern = re.compile('(<!--.*?-->)|(<!--)|(-->)')

#括号
quotePattern = re.compile('(\')|(\")|(\\\\)')
#infobox
infoboxPattern = re.compile('\{\{.*?\'{3}',re.DOTALL)
#表格
tablePattern = re.compile('(\{\|.*?\|\})|(\{\{.*?\}\})',re.DOTALL)
#角注
refPattern = re.compile('(<ref.*?>.*?</ref>)|(<ref .*?/>)',re.DOTALL)



xml_xsi = "{http://www.mediawiki.org/xml/export-0.8/}" 

DBHostAddress = 'localhost'
DBUsername = 'root'
DBPassword = '627116'
DBDatabasename = 'test'

