# -*- coding:utf-8 -*-
'''
Created on Dec 20, 2013

@author: lanny
'''
import xml.etree.ElementTree as ET
import sys
import time
import re    #正则表达式模块
import Stemmer
import string
#pge-articels xml文档所在绝对路径

#运行前需要修改
#tempfilepath ='/eclipseworkspace/corpus/enwiki-20131104-pages-articles27.xml'
#tempfilepath ='wikiexample.xml'
#生成的SQL文档所在目录绝对路径
#运行前需要修改
tempoutputfiledirpath = "/home/lanny/Documents/project/wiki_xml_doc/test/outSQL/"
tempfilepath ='/home/lanny/Documents/project/wiki_xml_doc/enwiki-20131104-pages-articles27.xml'

#维基百科管理标题
wikiPattern = re.compile('(Category:)?.*?((w|W)iki(P|p)edia.*?)')
#网页链接
websitePattern = re.compile('(\w+\.){2,4}\w+')
#类别标签
categoryPattern = re.compile(u'\[\[Category:.+?\]\]')
#消歧义类别模板
disambiguationPattern = re.compile(u'\{\{((.*?(D|d)isambig.*?)|(.*?(H|h)ndis.*?)|(.*?(G|g)eodis.*?)|(.*?(L|l)ists of ambiguous numbers.*?))\}\}')

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
#XML文档的一些基本消息
class XMLModel:
    def __init__(self):
        self.dumpFilePath = tempfilepath                 
        self.outputFileDirPath = tempoutputfiledirpath   
        self.xsi = "{http://www.mediawiki.org/xml/export-0.8/}" 
        self.namespace = dict()
#对文档进行处理
class XMLProcessor:
    def __init__(self):
        self.model = XMLModel()
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
            self.namespaceSqlFile = open(self.model.outputFileDirPath + 'namespace.sql','w')
            self.articleSqlFile = open(self.model.outputFileDirPath + 'articles.sql','w') 
            self.leafCatSqlFile = open(self.model.outputFileDirPath+'leafCat.sql','w')
            self.categorySqlFile = open(self.model.outputFileDirPath + 'category.sql','w')
            self.disambiguateSqlFile = open(self.model.outputFileDirPath+'disambiguate.sql','w')
            self.pagelinksSqlFile = open(self.model.outputFileDirPath+'pagelinks.sql','w')
            self.redirectSqlFile = open(self.model.outputFileDirPath+'redirect.sql','w')
        except:
            print "Could not open output file!"
        
        createNamespaceTableSql = '''
             DROP TABLE IF EXISTS `namespace`;\n
                CREATE TABLE `namespace` (`id` int(100) unsigned NOT NULL AUTO_INCREMENT,`title` varchar(1000) DEFAULT NULL,PRIMARY KEY (`id`),KEY `title` (`title`(15))) ENGINE=InnoDB AUTO_INCREMENT=230395 DEFAULT CHARSET=utf8mb4;\n '''
        createArticleTableSql = '''
               DROP TABLE IF EXISTS `article`;\n
                CREATE TABLE `article` (`id` int(100) unsigned NOT NULL AUTO_INCREMENT,`title` varchar(1000) DEFAULT NULL,`abstract` longtext DEFAULT NULL,`anchor` longtext DEFAULT NULL,`text`  longtext ,`anchor2` longtext ,PRIMARY KEY (`id`),KEY `title` (`title`(15)))ENGINE=InnoDB AUTO_INCREMENT=353354 DEFAULT CHARSET=utf8mb4;\n '''
        createLeafCatTableSql = '''DROP TABLE IF EXISTS `leafCat`;\n
                CREATE TABLE `leafCat` ( `id` int(100) unsigned NOT NULL AUTO_INCREMENT,`title` varchar(1000) DEFAULT NULL,`leafcat` longtext DEFAULT NULL,PRIMARY KEY (`id`), KEY `title` (`title`(15))) ENGINE=InnoDB AUTO_INCREMENT=332485 DEFAULT CHARSET=utf8mb4;\n'''
        createCategoryTableSql = '''DROP TABLE IF EXISTS `category`;\n
                CREATE TABLE `category` (`id` int(100) unsigned NOT NULL AUTO_INCREMENT,`cat_title` varchar(1000) DEFAULT NULL,`p_cat` longtext DEFAULT NULL,`c_cat` varchar(1000) DEFAULT NULL,PRIMARY KEY (`id`), KEY `title` (`cat_title`(15))  ) ENGINE=InnoDB AUTO_INCREMENT=77953 DEFAULT CHARSET=utf8mb4;\n'''
        createDisambiguateTableSql = '''
                DROP TABLE IF EXISTS `disambiguation`;\n
                CREATE TABLE `disambiguation` (`id` int(100) unsigned NOT NULL AUTO_INCREMENT,`title` varchar(1000) DEFAULT NULL,`dis_title` longtext DEFAULT NULL,`dis_id` longtext DEFAULT NULL, PRIMARY KEY (`id`),KEY `title` (`title`(15)))ENGINE=InnoDB AUTO_INCREMENT=230395 DEFAULT CHARSET=utf8mb4;\n'''
        createPagelinksTableSql= '''
                DROP TABLE IF EXISTS `pagelinks`;\n
                CREATE TABLE `pagelinks` (`source_id` int(100) unsigned NOT NULL , `source_title` longtext DEFAULT NULL ,`target_id` int(100) unsigned DEFAULT NULL, `target_title` longtext DEFAULT NULL,`outlinknumber` int(100) unsigned DEFAULT NULL,`inlinknumber` int(100) unsigned DEFAULT NULL ,PRIMARY KEY (`source_id`))ENGINE=InnoDB AUTO_INCREMENT=230395 DEFAULT CHARSET=utf8mb4;\n'''

        createRedirectTableSql = '''
                 DROP TABLE IF EXISTS `redirect`;\n
                 CREATE TABLE `redirect` ( `id` int(100) unsigned NOT NULL AUTO_INCREMENT,`title` varchar(1000) DEFAULT NULL , `rd_title` varchar(1000) DEFAULT NULL, PRIMARY KEY (`id`), KEY `title` (`title`(15)) )ENGINE=InnoDB AUTO_INCREMENT=230395 DEFAULT CHARSET=utf8mb4;\n '''
        
        try:
            self.namespaceSqlFile.write(createNamespaceTableSql)
            self.articleSqlFile.write(createArticleTableSql)
            self.leafCatSqlFile.write(createLeafCatTableSql)
            self.categorySqlFile.write(createCategoryTableSql)
            self.disambiguateSqlFile.write(createDisambiguateTableSql)
            self.pagelinksSqlFile.write(createPagelinksTableSql)
            self.redirectSqlFile.write(createRedirectTableSql)
                   
        except:
            print "Cannot write preprocess data to output file"
        
    
    #把打开的SQL文件关闭
    def close(self):
        self.redirectSqlFile.close()
        self.categorySqlFile.close()
        self.leafCatSqlFile.close()
        self.disambiguateSqlFile.close()
        self.articleSqlFile.close()
        self.pagelinksSqlFile.close()
        self.namespaceSqlFile.close()
        
    #加上引号，语句符合SQL格式            
    def changeToStr(self,from_elem):
        if type(from_elem) is str: 
            a = '\''
            b = '\'\''
            from_elem = from_elem.replace(a,b)
        return '\''+str(from_elem)+'\''
    
     #这个函数可忽略，不用理 
    def getContent(self,the_elem):
        if the_elem != None:
            print the_elem
            for each_sub in the_elem:
                self.getContent(each_sub)
                
    def getNameSpace(self,elem):
        for subelem in elem.findall("./"+self.model.xsi+"namespaces"):
            for ns in subelem.findall("./"+self.model.xsi+"namespace"):                
                if ns.text == None:
                    #主空间的key为0                    
                    self.model.namespace[ ns.attrib["key"] ] = (self.model.xsi)
                else:
                    self.model.namespace[ ns.attrib["key"] ] = (self.model.xsi + ns.text)
    
    def getRedirectInsertSql(self,title_id,from_title,to_title):
        if(from_title != None and to_title != None):
            return "INSERT INTO `redirect` VALUES ("+'\''+str(title_id)+'\''+','+'\''+from_title+'\''+',' + '\''+to_title+'\''+');\n'
        elif from_title == None:
            return "INSERT INTO `redirect` VALUES ("+'\''+str(title_id)+'\''+','+'\'\''+',' + '\''+to_title+'\''+');\n'
        else:
            return "INSERT INTO `redirect` VALUES ("+'\''+str(title_id)+'\''+','+'\''+from_title+'\''+',' + '\'\''+');\n'
   
    #得到插入到leafCategory表的插入语句
    
    def getLeafCategoryInsertSql(self,record_id,title,category):
        if(title != None and category != None):
            return "INSERT INTO `leafCat` VALUES ("+'\''+str(record_id)+'\''+','+'\''+title+'\''+','+'\'' + category+'\''+');\n'
        elif title == None:
            return "INSERT INTO `leafCat` VALUES ("+'\''+str(record_id)+'\''+','+'\'\''+',' +'\''+ category+'\''+');\n'
        else:
            return "INSERT INTO `leafCat` VALUES ("+'\''+str(record_id)+'\''+','+title+',' +'\'\''+');\n'
    def getCategoryInsertSql(self,title_id,cat_title,cats):
        cat_title = cat_title[9:]
        if(cat_title != None and cats != None):
            return "INSERT INTO `category` VALUES ("+'\''+str(title_id)+'\''+','+'\''+cat_title+'\''+','+'\'' + cats+'\''+','+'\'\''+');\n'
        elif cat_title == None:
            return "INSERT INTO `category` VALUES ("+'\''+str(title_id)+'\''+','+'\'\''+',' +'\''+ cats+'\''+','+'\'\''+');\n'
        else:
            return "INSERT INTO `category` VALUES ("+'\''+str(title_id)+'\''+','+cat_title+',' +'\'\''+','+'\'\''+');\n'
    def getArticleSql(self,pageid,pagetitle,abs,anchor,text,anchor2):
        values= (pageid,pagetitle,abs,'|'.join(anchor),text,'|'.join(anchor2))
        
        return "INSERT INTO `article` VALUES('%s','%s','%s','%s','%s','%s');\n"%values                 
    def getPagelinkSql(self,pageid,title,targettitle):
        values = (pageid,title,0,'|'.join(targettitle),-1,-1)
        return "INSERT INTO `pagelinks` VALUES('%s','%s','%s','%s','%s','%s');\n"%values
    
    def getDisambiguationSql(self,pageid,pagetitle,dis_titles):
        values = (pageid,pagetitle,'|'.join(dis_titles),0)
        return  "INSERT INTO `disambiguation` VALUES('%s','%s','%s','%s');\n"%values    
    
   
    def processRedirect(self,elem,redirectSubelem,title_id):
        global quotePattern
        from_title = re.sub(quotePattern,'\'\'',elem[0].text)
        to_title = re.sub(quotePattern,'\'\'',redirectSubelem.attrib["title"])
        #print from_title,to_title
        try:  
            self.redirectSqlFile.write(self.getRedirectInsertSql(title_id,from_title,to_title))
        except:
            print "Cann't write to the redirectSqlFile"
    def processNamespace(self,pageid,title):
        insertNamespaceSql = "INSERT INTO `namespace` VALUES ('%s','%s');\n"%(pageid,title)
        try:
            self.namespaceSqlFile.write(insertNamespaceSql)
        except:
            print "cannot write to the namespace file"
    #去掉大括号
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
    def processText(self,elem,textSubelem,page_id,catFlag= False):
        global categoryPattern,innerLinkPattern
        global partionPattern,skipPattern,htmltagPattern,filePattern,removeBracket
        global quotePattern,disambiguationPattern
        global bracketPattern,tablePattern
        global wikiPattern,refPattern,commentPattern
        if textSubelem.text != None:
            title = re.sub(quotePattern,'\'\'',elem[0].text)
            disFlag = False
            catlist = []
            text = textSubelem.text
            if page_id == 29673634:
                print textSubelem.text
                sys.exit()
#             if text[-1] == '}':
#                 print text[-30:-1],title
            if len(text) > 501:
                last = str(text)[-500:]
            else:
                last = str(text)
            last = last.split('\n',-1) 
            #查找消歧标记
            for eachline in last:
                match = re.match(disambiguationPattern,eachline)
                if match:
                    disFlag = True
                    oneCat = re.sub(quotePattern,'\'\'',match.group(0)[2:-2])    
                    #print oneCat                
                    catlist.append(oneCat)
            text = re.sub(skipPattern,'\n[[Category:',text)
            
            text = self.removeBracket(text)
            text = re.sub(filePattern,'',text)  #去掉非文本
            text = re.sub(refPattern,'',text)   # 去掉脚注
            text = re.sub(commentPattern,'',text)            
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
                    isPartion = re.match(partionPattern,line)
                    if isPartion:
                        absFlag = False
                                  
                #匹配其类别
                catm  = re.match(categoryPattern,line)
                if catm:
                    oneCat = catm.group(0)[11:-2]
                    #去掉wiki分类
                    if re.match(wikiPattern,oneCat):
                        continue                     
                    string.replace(oneCat,'|','')
                    string.replace(oneCat,'*','')
                    oneCat = re.sub(quotePattern,'\'\'',oneCat)
                    if len(oneCat) !=0 :
                        catlist.append(oneCat)
                    else:
                        print oneCat
                    if disFlag == False:
                        if re.match(disambiguationPattern,oneCat):
                            disFlag = True
                    
                    continue
                
                line = re.sub(quotePattern,'\'\'',line)    
                #如果存在摘要
                if absFlag:               
                    absAnchorlist += re.findall(innerLinkPattern,line)
                    line=re.sub(htmltagPattern,'',line)
                    if line == '':
                        continue
                    line = re.sub(bracketPattern,'',line)
                    absRawText += line
                
                else:
                    textAnchorlist += re.findall(innerLinkPattern,line)
                    line=re.sub(htmltagPattern,'',line)
                    line = re.sub(bracketPattern,'',line)
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
                if catFlag == False:
                    self.articleSqlFile.write(self.getArticleSql(page_id,title,absRawText,absAnchorlist,textRawText,textAnchorlist))
                    self.leafCatSqlFile.write(self.getLeafCategoryInsertSql(page_id,title,'|'.join(catlist)))
                    self.pagelinksSqlFile.write(self.getPagelinkSql(page_id,title, absAnchorlist+textAnchorlist))
            except:
                print "Cannot write to the output file"
                sys.exit(1)
    
    #主要处理过程
    def Processor(self):
        global wikiPattern
        #程序开始运行时间
        sTime= time.time()
        #预处理
        self.OutputFilePreprocess()
        
        #暂时还未用到    
        events = ("start","end")
        count = 0
        #调用ElementTree
        #parser = ET.XMLParser(encoding="utf-8")
        #content = ET.iterparse(self.model.dumpFilePath,events,parser)
        
        try:
            parser = ET.XMLParser(encoding="utf-8")
            
            content = ET.iterparse(self.model.dumpFilePath,events,parser)
            content = iter(content)
            event,root = content.next()

        except:
            print "Can't open dump file"
            sys.exit(1)
        
        pageid = 0  
        categoryid = 0
        for (event,elem) in content:
            
            if elem.tag ==self.model.xsi+"siteinfo" :
                self.getNameSpace(elem)
                continue
            
            if elem.tag == self.model.xsi+"page":
                #得到页面的ns分类节点
                nsSubelem = elem.find("./"+self.model.xsi+"ns")
                #得到每一页的text所在节点
                textSubelem = elem.find("./"+self.model.xsi+"revision/"+self.model.xsi+"text")
                
                if nsSubelem != None:
                    # =0 表示这是主要的文章，
                    if nsSubelem.text  == "0":
                        if len(elem) >2 :
                            if elem[2].text == None:
                                #print elem[0].text
                                continue
                            #去掉包含wikipedia 的页面 
                            if re.match(wikiPattern,elem[0].text):
                                continue
                            title = re.sub(quotePattern,'\'\'',elem[0].text)
                            pageid = int(elem[2].text)
                            self.processNamespace(pageid,title)
                            redirectSubelem = elem.find("./"+self.model.xsi+"redirect")
                            if redirectSubelem != None :
                                 self.processRedirect(elem,redirectSubelem,pageid)
                                 continue
                    # =14 表示这是维基百科内置的分类，可由这些页面找到他们的父亲类
                    if(textSubelem != None):
                        
                            #去掉包含wikipedia 的页面
                        if re.match(wikiPattern,elem[0].text):
                                continue
                        if nsSubelem.text =='14':
                            categoryid +=1
                            self.processText(elem, textSubelem, categoryid, True)
                        elif nsSubelem.text == '0':
                            #print pageid
                            #print textSubelem.text
                            self.processText(elem, textSubelem, pageid)
            elem.clear()
            root.clear()
        eTime = time.time()
        print str((eTime - sTime)/60) +" min"

        #关闭文件
        self.close()

#程序的入口
xmlProcessor = XMLProcessor()
xmlProcessor.Processor()
    
