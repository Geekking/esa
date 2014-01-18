# -*- coding:utf-8 -*-
#!/usr/bin/python
# Filename: PreProcess.py

'''
Created on Dec 20, 2013

@author: lanny
'''
import xml.etree.ElementTree as ET
import sys
import re    #正则表达式模块
import string
import ConstValue as cv
from SQLGenerator import SQLGenerator
#对文档进行处理
class XMLProcessor(SQLGenerator):
    def __init__(self):
        self.namespaceSqlFile = None
        self.articleSqlFile = None
        self.leafCatSqlFile = None
        self.categorySqlFile =None
        self.disambiguateSqlFile = None
        self.pagelinksSqlFile = None
        self.redirectSqlFile = None
    #对输出SQL文件进行进行预处理，主要是建表
    def OutputFilePreprocess(self):
        try:
            self.namespaceSqlFile = open(cv.outputFileDirPath + 'namespace.sql','w')
            self.articleSqlFile = open(cv.outputFileDirPath + 'articles.sql','w') 
            self.leafCatSqlFile = open(cv.outputFileDirPath+'leafCat.sql','w')
            self.categorySqlFile = open(cv.outputFileDirPath + 'category.sql','w')
            self.disambiguateSqlFile = open(cv.outputFileDirPath+'disambiguate.sql','w')
            self.pagelinksSqlFile = open(cv.outputFileDirPath+'pagelinks.sql','w')
            self.redirectSqlFile = open(cv.outputFileDirPath+'redirect.sql','w')
        except:
            print "Could not open output file!"     
        try:              
            self.namespaceSqlFile.write(self.getCreateNamespaceTableSql())
            self.articleSqlFile.write(self.getCreateArticleTableSql())
            self.leafCatSqlFile.write(self.getCreateLeafCatTableSql())
            self.categorySqlFile.write(self.getCreateCategoryTableSql())
            self.disambiguateSqlFile.write(self.getCreateDisambiguateTableSql())
            self.pagelinksSqlFile.write(self.getCreatePagelinksTableSql())
            self.redirectSqlFile.write(self.getCreateRedirectTableSql())

        except:
            print "Cannot write preprocess  data to output file"
            sys.exit(1)
    #把打开的SQL文件关闭
    def close(self):
        self.redirectSqlFile.close()
        self.categorySqlFile.close()
        self.leafCatSqlFile.close()
        self.disambiguateSqlFile.close()
        self.articleSqlFile.close()
        self.pagelinksSqlFile.close()
        self.namespaceSqlFile.close()
    def __del__(self):
        self.close()
    def processRedirect(self,from_title,to_title,title_id):
        from_title = re.sub(cv.quotePattern,'\'\'',from_title)
        to_title = re.sub(cv.quotePattern,'\'\'',to_title)
        try:  
            self.redirectSqlFile.write(self.getRedirectInsertSql(title_id,from_title,to_title))
        except:
            print "Cann't write to the redirectSqlFile"
    def processNamespace(self,pageid,title):
        title = re.sub(cv.quotePattern,'\'\'',title)
        try:
            self.namespaceSqlFile.write(self.getNamespaceInsertSql(pageid, title))
        except:
            print "cannot write to the namespace file"
            sys.exit(0)
    #去掉大括号,及其内部内容
    def removeBracket(self,text):
        firstIndex = 0
        lastIndex  = 0
        ans = ''
        brackets = []
        for index in range(len(text)):
            if text[index] == '{':
                if len(brackets) ==0:
                    lastIndex = index
                    if lastIndex != 0 :
                        ans += text[firstIndex:lastIndex]
                brackets.append('{')
            if text[index] == '}':
                if len(brackets) >0:
                    brackets.pop()
                
                if len(brackets) == 0:
                    firstIndex = index+1
        if firstIndex < len(text):
            ans += text[firstIndex:]
        return ans
    def disTemplateFinder(self,text):
        catlist = []
        if len(text) > 501:
            last = str(text)[-500:]
        else:
            last = str(text)
        last = last.split('\n',-1) 
        #查找消歧标记
        for eachline in last:
            match = re.match(cv.disambiguationPattern,eachline)
            if match:
                disFlag = True
                oneCat = re.sub(cv.quotePattern,'\'\'',match.group(0)[2:-2])    
                #print oneCat                
                catlist.append(oneCat)    
        return catlist            
    def processText(self,title,pageText,page_id,catFlag= False):
        if pageText != None:
            title = re.sub(cv.quotePattern,'\'\'',title)
            disFlag = False
            catlist = self.disTemplateFinder(pageText)
            if len(catlist) > 0:
                disFlag = True
            
            text = re.sub(cv.skipPattern,'\n[[Category:',pageText)
            text = self.removeBracket(text)
            text = re.sub(cv.filePattern,'',text)  #去掉非文本
            text = re.sub(cv.refPattern,'',text)   # 去掉脚注
            text = re.sub(cv.commentPattern,'',text)            
            result = text.split("\n")            
            absAnchorlist = []
            textAnchorlist = []
            absFlag = True
            absRawText = ''
            textRawText = ''
            for i in range(len(result)):
                line = result[i]
                if line == '':
                    continue
                if absFlag:
                    #判断是否存在摘要
                    isPartion = re.match(cv.partionPattern,line)
                    if isPartion:
                        absFlag = False
                                  
                #匹配其类别
                catm  = re.match(cv.categoryPattern,line)
                if catm:
                    oneCat = catm.group(0)[11:-2]
                    #去掉wiki分类
                    if re.match(cv.wikiPattern,oneCat):
                        continue                     
                    string.replace(oneCat,'|','')
                    string.replace(oneCat,'*','')
                    oneCat = re.sub(cv.quotePattern,'\'\'',oneCat)
                    if len(oneCat) !=0 :
                        catlist.append(oneCat)
                    else:
                        print oneCat
                    if disFlag == False:
                        if re.match(cv.disambiguationPattern,oneCat):
                            disFlag = True
                    
                    continue
        
                line = re.sub(cv.quotePattern,'\'\'',line)    
                #如果存在摘要
                if absFlag:               
                    absAnchorlist += re.findall(cv.innerLinkPattern,line)
                    line=re.sub(cv.htmltagPattern,'',line)
                    if line == '':
                        continue
                    line = re.sub(cv.bracketPattern,'',line)
                    absRawText += line
                
                else:
                    textAnchorlist += re.findall(cv.innerLinkPattern,line)
                    line=re.sub(cv.htmltagPattern,'',line)
                    line = re.sub(cv.bracketPattern,'',line)
                    if line == '':
                        continue
                    textRawText += line
                    
            for index in range(len(absAnchorlist)):
                absAnchorlist[index] = absAnchorlist[index][2:-2]               
            for index in range(len(textAnchorlist)):
                textAnchorlist[index] = textAnchorlist[index][2:-2]
            if disFlag:        
                try:
                    if catFlag == False:
                        self.disambiguateSqlFile.write(self.getDisambiguationSql(page_id,title, absAnchorlist +textAnchorlist ))
                except:
                    print "Could not write to disambiguate file"
                    sys.exit(1)
            try:
                if catFlag:
                    self.categorySqlFile.write(self.getCategoryInsertSql(page_id ,title,'|'.join(catlist)))
                else:
                    self.articleSqlFile.write(self.getArticleSql(page_id,title,absRawText,absAnchorlist,textRawText,textAnchorlist))
                    self.leafCatSqlFile.write(self.getLeafCategoryInsertSql(page_id,title,'|'.join(catlist)))
                    self.pagelinksSqlFile.write(self.getPagelinkSql(page_id,title, absAnchorlist+textAnchorlist))
            except:
                print "Cannot write to the output file"
                sys.exit(1)
        
    def getContent(self,content):
        page = {}
        (event,elem) = content.next()
        while True:
            if event =="end" and elem.tag == "{http://www.mediawiki.org/xml/export-0.8/}page":
                break
            if event == 'end':
                key = elem.tag[len(cv.xml_xsi):]
                if key == 'redirect':
                    value = elem.attrib["title"]
                else:
                    value = elem.text
                if key not in page.keys():
                    page[key] = value
            (event,elem) = content.next()
        return page
    def processPage(self,page,catFlag=False):
        title = page['title']
        pageid = page['id']
        text = page['text']
        if pageid == '29638504':
            print page
         #   sys.exit()
        if re.match(cv.wikiPattern,title):
            return 
        if not catFlag:
            self.processNamespace(pageid, title)
            if 'redirect' in page.keys():
                redirectTitle = page['redirect']
                self.processRedirect(title, redirectTitle, pageid)
                return 
            self.processText(title, text, pageid)
        else:
            self.processText(title, text, pageid, True)
    #主要处理过程
    def Processing(self):
        self.OutputFilePreprocess() 
        events = ("start","end")
        pageCount = 0
        try:
            parser = ET.XMLParser(encoding="utf-8")
            
            content = ET.iterparse(cv.dumpFilePath,events,parser)
            (event,root) = content.next()
        except:
            print "Can't open dump file"
            sys.exit(1)
        for (event,elem) in content:
            if event =="start" and elem.tag == cv.xml_xsi+'page':
                pageCount +=1
                if pageCount %4000 ==0:
                    print '已处理文章数目:'+str(pageCount)
                apage = self.getContent(content)
                if apage['ns'] == '0':
                    self.processPage(apage)
                elif apage['ns'] == '14':
                    self.processPage(apage,True)
                del apage
            root.clear()

        self.close()

    