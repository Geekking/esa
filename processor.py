# -*- coding:utf-8 -*-
'''
Created on Nov 24, 2013

@author: lanny
'''
import xml.etree.cElementTree as ET
import sys
import time
import re    #正则表达式模块
import Stemmer

STEMMER = Stemmer.Stemmer("porter")
#pge-articels xml文档所在绝对路径

#运行前需要修改
tempfilepath = "/home/lanny/Documents/project/wiki_xml_doc/enwiki-20131104-pages-articles1.xml"

#生成的SQL文档所在目录绝对路径
#运行前需要修改
tempoutputfiledirpath = "/home/lanny/Documents/project/wiki_xml_doc/test/outt/"



#XML文档的一些基本消息
class XMLModel:
    def __init__(self):
        self.dumpFilePath = tempfilepath                 
        self.outputFileDirPath = tempoutputfiledirpath   
        self.namespace = dict()                          
        self.xsi = "{http://www.mediawiki.org/xml/export-0.8/}" 
        
#对文档进行处理
class XMLProcessor:
    def __init__(self):
        self.model = XMLModel()
        self.redirectSqlFile = None
        self.categorySqlFile = None
        self.leafCatSqlFile = None
        self.wordlinkSqlFile =None
        self.rawWordsSqlFile = None
    #对输出SQL文件进行进行预处理，主要是建表
    def OutputFilePreprocess(self):
        try:
            self.redirectSqlFile = open(self.model.outputFileDirPath+"redirect.sql","w")
            self.categorySqlFile = open(self.model.outputFileDirPath+"category.sql","w")
            self.leafCatSqlFile = open(self.model.outputFileDirPath+"leafCategory.sql","w")
            self.wordlinkSqlFile = open(self.model.outputFileDirPath+"wordlinks.sql",'w')
            self.rawWordsSqlFile = open(self.model.outputFileDirPath +"rawWords.sql","w")
        except:
            print "Could open output file!"
            
        createRedirectTableSql = '''
                DROP TABLE IF EXISTS `redirect`;\n
                CREATE TABLE `redirect` (\n
                `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n
                `title` varchar(100) DEFAULT NULL,\n
                `rd_title` varchar(100) DEFAULT NULL,\n
                PRIMARY KEY (`id`),\n
                KEY `title` (`title`(15))\n
                )ENGINE=InnoDB AUTO_INCREMENT=230395 DEFAULT CHARSET=utf8;\n\n '''
        
        createLeafCatTableSql =  '''DROP TABLE IF EXISTS `leafcat`;\n
                CREATE TABLE `leafcat` (\n 
                `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n
                `title` varchar(100) DEFAULT NULL,\n
                `cat` varchar(1000) DEFAULT NULL,\n PRIMARY KEY (`id`),\n 
                KEY `title` (`title`(15))\n
                ) ENGINE=InnoDB AUTO_INCREMENT=332485 DEFAULT CHARSET=utf8;\n\n'''
        
        
        createCategoryTableSql = '''DROP TABLE IF EXISTS `cat`;\n
                CREATE TABLE `cat` (\n
                `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n
                `cat_title` varchar(100) DEFAULT NULL,\n
                `cat` varchar(1000) DEFAULT NULL,\nPRIMARY KEY (`id`),\n
                KEY `title` (`cat_title`(15)) \n
                ) ENGINE=InnoDB AUTO_INCREMENT=77953 DEFAULT CHARSET=utf8;\n\n'''

        createWordlinkTableSql = ["SET FOREIGN_KEY_CHECKS=0;\n",
                                "-- ----------------------------",
                                "-- Table structure for wordlinks\n",
                                "-- ----------------------------\n",
                                "DROP TABLE IF EXISTS `wordlinks`;\n",
                                "CREATE TABLE `wordlinks` (\n",
                                "       `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n",
                                "       `title` varchar(100) DEFAULT NULL,\n",
                                "      `abstract` varchar(2000) DEFAULT NULL,\n",
                                "      `text` longtext,\n",
                                "      PRIMARY KEY (`id`),\n",
                                "       KEY `title` (`title`(15))\n",
                                ") ENGINE=InnoDB AUTO_INCREMENT=353354 DEFAULT CHARSET=utf8;\n"]
        
        createWordlinkTableSql = "".join(createWordlinkTableSql)

        createRawWordsTableSql = '''
            SET FOREIGN_KEY_CHECKS=0;
            DROP TABLE IF EXISTS 'rawWords';
            CREATE TABLE 'rawWords'(
                'id' int(10)unsigned NOT NULL AUTO_INCREMENT,
                'title' varchar(100) DEFAULT NULL,
                'rawAbstract' varchar(2000) DEFAULT NULL,
                'rawText' longtext,
                PRIMARY KEY('id'),
                KEY 'title' ('title'(15))
            )ENGINE=InnoDB AUTO_INCREMENT=353354 DEFAULT CHARSET=utf8;
        
        '''
        try:
            self.redirectSqlFile.write(createRedirectTableSql)
            self.leafCatSqlFile.write(createLeafCatTableSql)
            self.categorySqlFile.write(createCategoryTableSql)
            self.wordlinkSqlFile.write(createWordlinkTableSql)
            self.rawWordsSqlFile.write(createRawWordsTableSql)
        except:
            print "Cannot write preprocess data to output file"
        
    def getRedirectInsertSql(self,title_id,from_title,to_title):
        if(from_title != None and to_title != None):
            return "INSERT INTO `redirect` VALUES ("+'\''+str(title_id)+'\''+','+'\''+from_title+'\''+',' + '\''+to_title+'\''+');\n'
        elif from_title == None:
            return "INSERT INTO `redirect` VALUES ("+'\''+str(title_id)+'\''+','+'\'\''+',' + '\''+to_title+'\''+');\n'
        else:
            return "INSERT INTO `redirect` VALUES ("+'\''+str(title_id)+'\''+','+'\''+from_title+'\''+',' + '\'\''+');\n'
 
    #把打开的SQL文件关闭
    def close(self):
        self.redirectSqlFile.close()
        self.categorySqlFile.close()
        self.leafCatSqlFile.close()   
        self.wordlinkSqlFile.close()
        self.rawWordsSqlFile.close()
    #这个函数可忽略，不用理 
    def getContent(self,the_elem):
        if the_elem != None:
            print the_elem
            for each_sub in the_elem:
                self.getContent(each_sub)
    
    #加上引号，语句符合SQL格式            
    def changeToStr(self,from_elem):
        if type(from_elem) is str: 
            a = '\''
            b = '\'\''
            from_elem = from_elem.replace(a,b)
        return '\''+str(from_elem)+'\''
    #得到插入到wordlist表的一条语句，
    
    def getWordListInsertSql(self,title_id,title,abstractWordList,textWordList,):
        insertSql = "INSERT INTO `wordlinks` VALUES ("
        titleStr = self.changeToStr(title)
        titleIDStr = self.changeToStr(title_id)
        if len(abstractWordList)>0 :
            abstractWords = abstractWordList[0][2:-2]
            index = 1
            while index<len(abstractWordList):
                abstractWords += ','+abstractWordList[index][2:-2]
                index += 1
            abstractWords = self.changeToStr(abstractWords)
        else:
            abstractWords ='\'\''
        
        if len(textWordList)>0:
            textWords = textWordList[0][2:-2]
            index =1
            while index < len(textWordList):
                textWords +=','+ textWordList[index][2:-2]
                index += 1
            textWords = self.changeToStr(textWords)
        else:
            textWords = "\'\'"
        #print insertSql + titleIDStr+','+titleStr+','+abstractWords + ','+textWords +')\n'
        return insertSql + titleIDStr+','+titleStr+','+abstractWords + ','+textWords +');\n'
            
    #
    
    #得到插入到leafCategory表的插入语句
    
    def getLeafCategoryInsertSql(self,record_id,page,category,):
        if(page != None and category != None):
            return "INSERT INTO `leafcat` VALUES ("+'\''+str(record_id)+'\''+','+'\''+page+'\''+','+'\'' + category+'\''+');\n'
        elif page == None:
            return "INSERT INTO `leafcat` VALUES ("+'\''+str(record_id)+'\''+','+'\'\''+',' +'\''+ category+'\''+');\n'
        else:
            return "INSERT INTO `leafcat` VALUES ("+'\''+str(record_id)+'\''+','+page+',' +'\'\''+');\n'
    
    
    def getCategoryInsertSql(self,title_id,cat_title,cats):
        
        if(cat_title != None and cats != None):
            return "INSERT INTO `cat` VALUES ("+'\''+str(title_id)+'\''+','+'\''+cat_title+'\''+','+'\'' + cats+'\''+');\n'
        elif cat_title == None:
            return "INSERT INTO `cat` VALUES ("+'\''+str(title_id)+'\''+','+'\'\''+',' +'\''+ cats+'\''+');\n'
        else:
            return "INSERT INTO `cat` VALUES ("+'\''+str(title_id)+'\''+','+cat_title+',' +'\'\''+');\n'
                
    def getRawWordsInsertSql(self,page_id,page_title,rawAbstractWord,rawText):
        insertSql = "INSERT INTO `rawWords` VALUES ("
        titleStr = self.changeToStr(page_title)
        titleIDStr = self.changeToStr(page_id)
        rawAbstractWordStr = self.changeToStr(rawAbstractWord)
        rawTextStr = self.changeToStr(rawText)
        
        return insertSql + titleIDStr+','+titleStr+','+rawAbstractWordStr + ','+rawTextStr +');\n'
        
    #对重定向信息进行处理   
    def processRedirect(self,elem,redirectSubelem,title_id):
        from_title = elem[0].text
        to_title = redirectSubelem.attrib["title"]
        #print from_title,to_title
        try:  
            self.redirectSqlFile.write(self.getRedirectInsertSql(title_id,from_title,to_title))
        except:
            print "Cann't write to the redirectSqlFile"
                

    #找出文档中用到的命名空间，主要时区分page是输入那一类，这里的类别不是文档的类别   
    def getNameSpace(self,elem):
        for subelem in elem.findall("./"+self.model.xsi+"namespaces"):
            for ns in subelem.findall("./"+self.model.xsi+"namespace"):
                if ns.text == None:
                    self.model.namespace[ ns.attrib["key"] ] = (self.model.xsi)
                else:
                    self.model.namespace[ ns.attrib["key"] ] = (self.model.xsi + ns.text)
    
    #给页面加上分类消息
    def processLeafCategory(self,elem,textSubelem,page_id):
        categoryPattern = re.compile(u'\[\[Category:[:_a-zA-Z\u4E00-\u9FA5\uF900-\uFA2D\|\-\*\(\) ]*?\]\]')
        if textSubelem.text != None:
            #print textSubelem.text
            result = textSubelem.text.split("\n")
            catlist = []
            for i in result:
                m = re.match(categoryPattern,i.decode("utf8"))
                if not m == None:
                    catlist.append(m.group(0))
            if len(catlist) >0:
                cat = catlist[0][2:-2]
                categorys = cat
                index = 1
                while index < len(catlist):
                    cat = catlist[index][2:-2]
                    index += 1
                    if cat == "*" or cat == "":
                        continue
                    if cat[0] !='|': 
                        oneCat =  '|'+cat
                    else:
                        oneCat = cat
                    categorys += oneCat
                try:  
                    self.leafCatSqlFile.write(self.getLeafCategoryInsertSql(page_id,elem[0].text,categorys))
                except:
                    print "Cann't write to the leafcategorySqlFile"
                
    #给每一个分类，加上分类信息    
    def processCategory(self,elem,textSubelem,page_id):
        categoryPattern = re.compile(u'\[\[Category:[:_a-zA-Z\u4E00-\u9FA5\uF900-\uFA2D\|\-\*\(\) ]*?\]\]')
        if textSubelem.text != None:
            #分离成每一行进行处理
            result = textSubelem.text.split("\n")
            #存放所有分类
            catlist = []
            
            for i in result:
                m = re.match(categoryPattern,i.decode("utf8"))
                if not m == None:
                    catlist.append(m.group(0))
            
            if len(catlist) >0:
                #抽出类别名称
                cat = catlist[0][2:-2]
                categorys = cat
                index = 1
                
                while index < len(catlist):
                    cat = catlist[index][2:-2]
                    index += 1
                    if cat == "*" or cat == "":
                        continue
                  
                    if cat[0]!='|':
                        oneCat =  '|'+cat
                    else:
                        oneCat = cat
                    categorys += oneCat
                
                if elem[0].text != None:
                    #print elem[0].text
                    (pre,cat_title) = elem[0].text.split(":",1)
                else:
                    cat_title = ""
                    
                try:  
                    self.categorySqlFile.write(self.getCategoryInsertSql(page_id,cat_title,categorys))
                except:
                    print "Cann't write to the CategorySqlFile"
    
    #对每一篇page，把它的摘要和正文中有链接的词抽出来
    def processWordLink(self,elem,textSubelem,title_id):
        
        #正则表达式
        wordPattern = re.compile(u'\[\[[a-zA-Z\u4E00-\u9FA5\uF900-\uFA2D\|\-\(\) ]*?\]\]')
        
        if textSubelem.text != None:
            if not str(textSubelem.text).find("==")==-1:
                
                (abstract,text) = str(textSubelem.text).split("==",1)
                abstractWordList = re.findall(wordPattern,abstract.decode("utf8"))
                textWordList = re.findall(wordPattern,text.decode("utf8"))
                try:
                    self.wordlinkSqlFile.write(self.getWordListInsertSql(title_id,str(elem[0].text),abstractWordList, textWordList))
                except:
                    print "Cannot write the Word link file"

    def processRawWords(self,elem,textSubelem,page_id):
        if textSubelem.text != None :
            if str(textSubelem.text).find("==") == -1:
                return
            (abstract,text) = str(textSubelem.text).split("==",1)
            #abstract = abstract.decode('utf-8')
            #text = text.decode("utf-8")
            try:
                print abstract,text
                
                self.rawWordsSqlFile.write(self.getRawWordsInsertSql(page_id,str(elem[0].text), abstract, text))
            except:
                print "Cannot write to raw words file"
            
    #主要处理过程
    def Processor(self):
        #程序开始运行时间
        sTime= time.time()
        #预处理
        self.OutputFilePreprocess()
        
        #暂时还未用到    
        events = ("start","end")
        
        #调用ElementTree
        try:
            content = ET.iterparse(self.model.dumpFilePath,events)
        except:
            print "Can't open dump file"
        
        page_count=1
        category_count = 1
        
        for (event,elem) in content:
            '''
            #这里这样处理有错误，TO DO：忽略掉xsi，直接给他一个初始值
            if elem.tag == self.model.xsi+"wikimedia":
                self.model.xsi = elem.attrib["xsi:schemaLocation"]
                continue
            '''
            

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
                        redirectSubelem = elem.find("./"+self.model.xsi+"redirect")
                        if redirectSubelem != None :
                            self.processRedirect(elem,redirectSubelem,page_count)
                            
                    # =14 表示这是维基百科内置的分类，可由这些页面找到他们的父亲类
                    if(textSubelem != None):
                        if nsSubelem.text =='14':
                            self.processCategory(elem, textSubelem, category_count)
                            category_count +=1
                            continue;
                    
                
                if(textSubelem != None):
                    self.processLeafCategory(elem, textSubelem, page_count)
                    self.processWordLink(elem, textSubelem, page_count)
                    self.processRawWords(elem, textSubelem, page_count)
                page_count += 1
            
            #！！！这里很重要，这里可以节省很大内存，对处理过的节点，直接丢弃           
            elem.clear()
        
        eTime = time.time()
        print str((eTime - sTime)) +"sec"

        #关闭文件
        self.close()

#程序的入口
xmlProcessor = XMLProcessor()
xmlProcessor.Processor()
    