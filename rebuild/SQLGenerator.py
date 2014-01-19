#!/usr/bin/python
# Filename: SQLGenerator.py
#-*- coding:utf-8 -*-

'''
Created on Jan 18, 2014

@author: lanny
'''

class SQLGenerator:
    def changeToStr(self,from_elem):
        if type(from_elem) is str: 
            a = '\''
            b = '\'\''
            from_elem = from_elem.replace(a,b)
        return '\''+str(from_elem)+'\''
    def getNamespaceInsertSql(self,pageid,title):
        return "INSERT INTO `namespace` VALUES ('%s','%s');\n"%(pageid,title)
    def getRedirectInsertSql(self,title_id,from_title,to_title):
        if(from_title != None and to_title != None):
            return "INSERT INTO `redirect` VALUES ("+'\''+str(title_id)+'\''+','+'\''+from_title+'\''+',' + '\''+to_title+'\''+');\n'
        elif from_title == None:
            return "INSERT INTO `redirect` VALUES ("+'\''+str(title_id)+'\''+','+'\'\''+',' + '\''+to_title+'\''+');\n'
        else:
            return "INSERT INTO `redirect` VALUES ("+'\''+str(title_id)+'\''+','+'\''+from_title+'\''+',' + '\'\''+');\n'
    def getLeafCategoryInsertSql(self,record_id,title,category):
        if(title != None and category != None):
            return "INSERT INTO `leafCat` VALUES ("+'\''+str(record_id)+'\''+','+'\''+title+'\''+','+'\'' + category+'\''+');\n'
        elif title == None:
            return "INSERT INTO `leafCat` VALUES ("+'\''+str(record_id)+'\''+','+'\'\''+',' +'\''+ category+'\''+');\n'
        else:
            return "INSERT INTO `leafCat` VALUES ("+'\''+str(record_id)+'\''+','+title+',' +'\'\''+');\n'
    def getArticleSql(self,pageid,pagetitle,abst,anchor,text,anchor2):
        values= (pageid,pagetitle,abst,'|'.join(anchor),text,'|'.join(anchor2))
        
        return "INSERT INTO `article` VALUES('%s','%s','%s','%s','%s','%s');\n"%values                 
    def getPagelinkSql(self,pageid,title,targettitle):
        values = (pageid,title,0,'|'.join(targettitle),-1,-1)
        return "INSERT INTO `pagelinks` VALUES('%s','%s','%s','%s','%s','%s');\n"%values
    
    def getDisambiguationSql(self,pageid,pagetitle,dis_titles):
        values = (pageid,pagetitle,'|'.join(dis_titles),0)
        return  "INSERT INTO `disambiguation` VALUES('%s','%s','%s','%s');\n"%values    
    def getCategoryInsertSql(self,title_id,cat_title,cats):
        cat_title = cat_title[9:]
        if(cat_title != None and cats != None):
            return "INSERT INTO `category` VALUES ("+'\''+str(title_id)+'\''+','+'\''+cat_title+'\''+','+'\'' + cats+'\''+','+'\'\''+');\n'
        elif cat_title == None:
            return "INSERT INTO `category` VALUES ("+'\''+str(title_id)+'\''+','+'\'\''+',' +'\''+ cats+'\''+','+'\'\''+');\n'
        else:
            return "INSERT INTO `category` VALUES ("+'\''+str(title_id)+'\''+','+cat_title+',' +'\'\''+','+'\'\''+');\n'

    def getCreateArticleTableSql(self):
        self.createArticleTableSql = '''
               DROP TABLE IF EXISTS `article`;\n
                CREATE TABLE `article` (`id` int(100) unsigned NOT NULL AUTO_INCREMENT,`title` varchar(1000) DEFAULT NULL,`abstract` longtext DEFAULT NULL,`anchor` longtext DEFAULT NULL,`text`  longtext ,`anchor2` longtext ,PRIMARY KEY (`id`),KEY `title` (`title`(15)))ENGINE=InnoDB AUTO_INCREMENT=353354 DEFAULT CHARSET=utf8mb4;\n '''
       
        return self.createArticleTableSql
    def getCreateNamespaceTableSql(self):
        self.createNamespaceTableSql = '''
             DROP TABLE IF EXISTS `namespace`;\n
                CREATE TABLE `namespace` (`id` int(100) unsigned NOT NULL AUTO_INCREMENT,`title` varchar(1000) DEFAULT NULL,PRIMARY KEY (`id`),KEY `title` (`title`(15))) ENGINE=InnoDB AUTO_INCREMENT=230395 DEFAULT CHARSET=utf8mb4;\n '''
      
        return self.createNamespaceTableSql
    def getCreateLeafCatTableSql(self):
        self.createLeafCatTableSql = '''DROP TABLE IF EXISTS `leafCat`;\n
                CREATE TABLE `leafCat` ( `id` int(100) unsigned NOT NULL AUTO_INCREMENT,`title` varchar(1000) DEFAULT NULL,`leafcat` longtext DEFAULT NULL,PRIMARY KEY (`id`), KEY `title` (`title`(15))) ENGINE=InnoDB AUTO_INCREMENT=332485 DEFAULT CHARSET=utf8mb4;\n'''
      
        return self.createLeafCatTableSql
    def getCreateCategoryTableSql(self):
        self.createCategoryTableSql = '''DROP TABLE IF EXISTS `category`;\n
                CREATE TABLE `category` (`id` int(100) unsigned NOT NULL AUTO_INCREMENT,`cat_title` varchar(1000) DEFAULT NULL,`p_cat` longtext DEFAULT NULL,`c_cat` varchar(1000) DEFAULT NULL,PRIMARY KEY (`id`), KEY `title` (`cat_title`(15))  ) ENGINE=InnoDB AUTO_INCREMENT=77953 DEFAULT CHARSET=utf8mb4;\n'''
  
        return self.createCategoryTableSql
    def getCreateDisambiguateTableSql(self):
        self.createDisambiguateTableSql = '''
                DROP TABLE IF EXISTS `disambiguation`;\n
                CREATE TABLE `disambiguation` (`id` int(100) unsigned NOT NULL AUTO_INCREMENT,`title` varchar(1000) DEFAULT NULL,`dis_title` longtext DEFAULT NULL,`dis_id` longtext DEFAULT NULL, PRIMARY KEY (`id`),KEY `title` (`title`(15)))ENGINE=InnoDB AUTO_INCREMENT=230395 DEFAULT CHARSET=utf8mb4;\n'''

        return self.createDisambiguateTableSql
    def getCreatePagelinksTableSql(self):
        self.createPagelinksTableSql= '''
                DROP TABLE IF EXISTS `pagelinks`;\n
                CREATE TABLE `pagelinks` (`source_id` int(100) unsigned NOT NULL , `source_title` longtext DEFAULT NULL ,`target_id` int(100) unsigned DEFAULT NULL, `target_title` longtext DEFAULT NULL,`outlinknumber` int(100)  DEFAULT NULL,`inlinknumber` int(100) DEFAULT NULL ,PRIMARY KEY (`source_id`))ENGINE=InnoDB AUTO_INCREMENT=230395 DEFAULT CHARSET=utf8mb4;\n'''

  
        return self.createPagelinksTableSql
    def getCreateRedirectTableSql(self):
        self.createRedirectTableSql = '''
                 DROP TABLE IF EXISTS `redirect`;\n
                 CREATE TABLE `redirect` ( `id` int(100) unsigned NOT NULL AUTO_INCREMENT,`title` varchar(1000) DEFAULT NULL , `rd_title` varchar(1000) DEFAULT NULL, PRIMARY KEY (`id`), KEY `title` (`title`(15)) )ENGINE=InnoDB AUTO_INCREMENT=230395 DEFAULT CHARSET=utf8mb4;\n '''
        
        return self.createRedirectTableSql
                             
        