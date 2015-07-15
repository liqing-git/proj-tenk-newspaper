# -*- coding: utf-8 -*-
'''
Created on 2015年6月12日

抽取频道首页或者每个公司新闻频道中的新闻
@author: liqing
'''

import time,traceback,os
import hashlib
from newspaper import Article
from crawl.basecrawl import BaseCrawl,BaseExtract
from config.logsetting import logger
from config.dbcon import get_cxn_db
from boilerpipe.extract import Extractor
from pybloom import BloomFilter

IDEL_TIME = 2
MAX_TRY_TIMES = 3
FINANCE_HOME = 'finance_home'
NEWS_URL_EXTRACTE = 0


def get_compnewsurls(code_names_file,url_prefix,finance_homes=None):
    '''产生上市公司新闻频道的urls
    @param code_names_file: 公司上市代码列表
    @param url_prefix: 拼接上市公司新闻url的前缀
    @param finance_homes: 财经频道首页地址
    '''
    if code_names_file is None or url_prefix is None:
        logger.error(' code_names_file or  url_prefix is None')
        return []
    news_chanel_urls = []
    if finance_homes and len(finance_homes) > 0:
        for f_home in finance_homes:
            news_chanel_urls.append((FINANCE_HOME,f_home))
    try:
        path = os.path.realpath(__file__)
        path = os.path.dirname(path)
        lspath = os.path.split(path)
        if lspath[0] and lspath[1]:
            code_names_file = lspath[0] + '/data/' + code_names_file
        
        
        for code_name in open(code_names_file):
            if code_name and len(code_name) > 0:
                cols = code_name.split('\t')
                if cols and len(cols) > 1:
                    news_chanel_urls.append((cols[0],url_prefix + cols[0]))
    except:
        logger.error("crawl_page failed ,Error:%s" % traceback.format_exc())     
    return news_chanel_urls


#构造一个布隆过滤器
bf = BloomFilter(capacity=1500000, error_rate=0.001)
def initUrlsBloomFilter(cursor,source_type):
    '''构造已经抓取新闻urls的布隆过滤器
    @param cursor: db链接
    @param source_type: 抓取来源：比如yahoo、google等
    '''
    sql = "select url from news_extract_content where crawl_source = " + str(source_type) + "  limit %s,%s"
    w_flag = True
    index = 0
    page_num = 50000
    while w_flag:
        w_flag = False
        count = cursor.execute(sql,(index,page_num))
        if count > 0:
            w_flag = True
        data = cursor.fetchall()
        for da in data :
            url = da[0]
            bf.add(url)
        index += page_num
            
    

class ExtracteNewsurls(object):
    """
    每个来源的频道中得新闻链接进行抽取，主要是可视化的抽取，因此每个来源可能不同
    """
    def extracte(self,news_chanel_url,is_finance_home,baseCrawl,baseExtract,req_referer,try_times = 1):
        '''抽取频道首页或者每个公司新闻频道中得所有urls,
        每个页面最多抓取 MAX_TRY_TIMES+1 次
        @param news_chanel_url: 抓取的链接，
        @param is_finance_home:是否是finance_home，可能需要单独处理
        @param baseCrawl: 封装的抓取基类
        @param baseExtract: 封装的抽取基类
        @param try_times: 重试次数
        @return: 抓取状态，抽取的新闻链接
        '''
        
        logger.info("crawl %s, %d time"%(news_chanel_url,try_times))
        time.sleep(IDEL_TIME * (try_times-1))
        status = 2
        html = ''
        try:
            status,html = baseCrawl.crawl_page(news_chanel_url, req_referer = req_referer)
            #返回频道下面所有新闻链接
            news_links = []
            if status == 200:
                status = 0
                logger.info("crawl %s, success "%news_chanel_url)
                news_links = self.extracteUrls(baseExtract,html, is_finance_home)
                
            elif status in [301,302]:
                status = 1
                logger.info("crawl %s, no data,fail"%news_chanel_url)
            else:
                if try_times <= MAX_TRY_TIMES:
                    return self.extracte(news_chanel_url,is_finance_home, baseCrawl, baseExtract, req_referer, try_times+1)
                else:
                    status = 2
                    logger.error("crawl %s, %d time,fail"%(news_chanel_url,try_times))
        except:
            logger.error("crawl %s, failed ,Error:%s" % (news_chanel_url,traceback.format_exc()))
        
        
        return status,news_links
        
        
        
    
    
    def extracteUrls(self,baseExtract,html,is_finance_home):
        """提取新闻链接
        @param baseExtract: 封装的抽取基类
        @param html:网页内容 
        @param is_finance_home: 是否是finance_home，可能需要单独处理
        """
        #必须子类实现
        raise NotImplementedError



def extract_news(code,news_links,crawl_source,cursor):
    '''抽取新闻，并进行NLP
    @param code: 上市公司编码
    @param news_links: 需要抽取的新闻链接
    @param crawl_source
    @param cursor: 数据库游标
    '''
    
    in_sql = """ INSERT INTO news_extract_content(url_md5,url,code_name,newspaper_title,newspaper_text,
newspaper_authors,newspaper_summary,newspaper_keywords,boilerpipe_article,
boilerpipe_articlesentences,boilerpipe_keepeverything,boilerpipe_largestcontent,
boilerpipe_numwordsrules,boilerpipe_canola,up_time,add_time,extract_count,crawl_source)
VALUES
(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now(),1,%s)
on duplicate key update code_name = %s,newspaper_title = %s,newspaper_text = %s,
newspaper_authors = %s,newspaper_summary = %s,newspaper_keywords = %s,
boilerpipe_article = %s,boilerpipe_articlesentences = %s,boilerpipe_keepeverything = %s,
boilerpipe_largestcontent = %s,boilerpipe_numwordsrules = %s,boilerpipe_canola = %s,
up_time = now(),extract_count=extract_count+1,crawl_source = %s """
     
    for link in news_links:
        #长度小于30的url一般都不是新闻连接,暴力，简单可依赖
        if link is None or len(link) <= 30:
            continue
        #已经抓取的url就不需要抓取了
        if link in bf:
            continue
        
        try:
            global NEWS_URL_EXTRACTE
            NEWS_URL_EXTRACTE += 1
            url_md5 = hashlib.md5(link).hexdigest()
            #首先让使用newspaper
            newspaper_title = ''
            newspaper_text = ''
            newspaper_authors = ''
            newspaper_summary = ''
            newspaper_keywords = ''
            article = Article(link)
            article.download()
            html = article.html
            if html is None or len(html) == 0:
                continue
            article.parse()
            if article.text and len(article.text) > 0:
                newspaper_title = article.title
                newspaper_text = article.text
                newspaper_authors = article.authors
                if newspaper_authors and len(newspaper_authors) > 0:
                    newspaper_authors = ','.join(newspaper_authors)
                else:
                    newspaper_authors = ''
                
                
                article.nlp()
                newspaper_summary = article.summary
                newspaper_keywords = article.keywords
                if newspaper_keywords and len(newspaper_keywords) > 0:
                    newspaper_keywords = ','.join(newspaper_keywords)
                else:
                    newspaper_keywords = ''
                
            #然后使用boilerpipe
            
            extractor = Extractor(extractor='ArticleExtractor',html = html)
            boilerpipe_article = extractor.getText()
            
            extractor = Extractor(extractor='ArticleSentencesExtractor',html = html)
            boilerpipe_articlesentences = extractor.getText()
            
            extractor = Extractor(extractor='KeepEverythingExtractor',html = html)
            boilerpipe_keepeverything = extractor.getText()
            
            extractor = Extractor(extractor='LargestContentExtractor',html = html)
            boilerpipe_largestcontent = extractor.getText()
            
            extractor = Extractor(extractor='NumWordsRulesExtractor',html = html)
            boilerpipe_numwordsrules = extractor.getText()
            
            extractor = Extractor(extractor='CanolaExtractor',html = html)
            boilerpipe_canola = extractor.getText()
            
            #输入的参数
            content = (url_md5,link,code, newspaper_title, newspaper_text, newspaper_authors,newspaper_summary,newspaper_keywords,\
                       boilerpipe_article,boilerpipe_articlesentences,boilerpipe_keepeverything,boilerpipe_largestcontent,\
                       boilerpipe_numwordsrules,boilerpipe_canola,crawl_source,   \
                       code, newspaper_title,newspaper_text, newspaper_authors,\
                       newspaper_summary,newspaper_keywords,boilerpipe_article,boilerpipe_articlesentences,boilerpipe_keepeverything,\
                       boilerpipe_largestcontent,boilerpipe_numwordsrules,boilerpipe_canola,crawl_source)
            cursor.execute(in_sql,content)
               
        except:
            logger.error("crawl_page failed ,Error:%s" % traceback.format_exc())
                
    

def go_newsextrac(source_type,code_names_file,url_prefix,req_referer,finance_homes,extracteNewsurls):
    '''抓取所有的新闻链接、抽取并存储
    @param source_type: 抓取来源：比如yahoo、google等
    @param code_names_file: 公司上市代码列表
    @param url_prefix: 拼接上市公司新闻url的前缀
    @param req_referer: 抓取前连，防封禁
    @param finance_homes: 财经频道首页地址
    @param  extracteNewsurls: 个性化的抽取对象
    '''
    baseCrawl = BaseCrawl() 
    baseExtract = BaseExtract()
    
    cxn_db = None
    try:
        cxn_db = get_cxn_db()
        cur_db = cxn_db.cursor()
        #构造一个boolm filter
        initUrlsBloomFilter(cur_db,source_type)
        
        in_sql = """ insert into com_news_extract_state (url,code_name,crawl_time,return_type,add_time,crawl_count,source_type)
values (%s,%s,now(),%s,now(),1,%s)
on duplicate key update crawl_time = now(),return_type = %s,crawl_count=crawl_count+1,source_type = %s """
       
        news_chanel_urls = get_compnewsurls(code_names_file,url_prefix,finance_homes)
        
        logger.info("number of companys is %d"%len(news_chanel_urls))
        for code,news_chanel_url in news_chanel_urls:
            logger.debug("crawl %s, start............ "%news_chanel_url)
            is_finance_home = False
            if code == FINANCE_HOME:
                is_finance_home = True
            status,news_links = extracteNewsurls.extracte(news_chanel_url,is_finance_home,baseCrawl,baseExtract,req_referer,try_times = 1)
            cur_db.execute(in_sql,(news_chanel_url,code,status,source_type,status,source_type))
            if status == 0 and len(news_links) > 0:
                extract_news(code,news_links,source_type,cur_db)
            logger.debug("crawl %s, end.............. "%code)
        
        logger.info("number of news url is %s"%NEWS_URL_EXTRACTE)
    
    except:
        logger.error("crawl_page failed ,Error:%s" % traceback.format_exc()) 
    finally:
        if cxn_db:
            cxn_db.close()
            
if __name__ == '__main__':
    cxn_db = None
    try:
        cxn_db = get_cxn_db()
        cur_db = cxn_db.cursor()
        #构造一个boolm filter
        initUrlsBloomFilter(cur_db,1)
        
#         print bf.exists('https://mail.yahoo.com/?.intl=us&.lang=en-US&.src=ym')
#         print bf.exists('http://www.latimes.com/la-fi-hy-helmet-safety-20150409-story.html')
#         print bf.exists('http://www.baidu.com')
        
    except:
        logger.error("crawl_page failed ,Error:%s" % traceback.format_exc()) 
    finally:
        if cxn_db:
            cxn_db.close()

