# -*- coding: utf-8 -*-
'''
Created on 2015年6月12日

@author: liqing
'''
import time,traceback,os
import hashlib
from newspaper import Article
from crawl.basecrawl import BaseCrawl,BaseExtract
from config.logsetting import logger
from config.dbcon import get_cxn_db
from boilerpipe.extract import Extractor

REQ_REFER = "http://finance.yahoo.com/"
IDEL_TIME = 2
MAX_TRY_TIMES = 3

def go_crawl_headlines(code_name,baseCrawl,baseExtract,try_times = 1):
    '''雅虎金融每个公司页面的新闻抓取的抓取,每个页面最多抓取 MAX_TRY_TIMES+1 次
    @param req_url: 抓取的链接，形如：http://finance.yahoo.com/q/h?s=bidu
    @param baseCrawl: 封装的抓取基类
    @param baseExtract: 封装的抽取基类
    @param try_times: 重试次数
    @return: 抓取状态，抽取的新闻链接，
    '''
    req_url = 'http://finance.yahoo.com/q/h?s=%s'%code_name
    
    logger.info("crawl %s, %d time"%(req_url,try_times))
    time.sleep(IDEL_TIME * try_times)
    status,html = baseCrawl.crawl_page(req_url, req_referer = REQ_REFER)
    #返回新闻链接
    headlines_links = []
    if status == 200:
        status = 0
        logger.info("crawl %s, success "%req_url)
        beautifulSoup = baseExtract.get_bs(html)
        content = beautifulSoup.find('div', {'class':'mod yfi_quote_headline withsky'})
        if content is None:
            logger.info("extract %s is none "%req_url)
        else:
            for link in beautifulSoup.findAll('a'):
                headlines_links.append(link.get('href'))
    elif status in [301,302]:
        status = 1
        logger.info("crawl %s, no data,fail"%req_url)
    else:
        if try_times <= MAX_TRY_TIMES:
            return go_crawl_headlines(req_url,baseCrawl,baseExtract,try_times+1)
        else:
            status = 2
            logger.error("crawl %s, %d time,fail"%(req_url,try_times))
    
    return status,headlines_links



def extract_headlines_news(code,headlines_links,cursor):
    '''抽取yahoo的新闻链接并解析'''
    
    in_sql = """ INSERT INTO yahoo_comp_news(url_md5,url,code_name,newspaper_title,newspaper_text,
newspaper_authors,newspaper_summary,newspaper_keywords,boilerpipe_article,
boilerpipe_articlesentences,boilerpipe_keepeverything,boilerpipe_largestcontent,
boilerpipe_numwordsrules,boilerpipe_canola,up_time,add_time,count)
VALUES
(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now(),1)
on duplicate key update code_name = %s,newspaper_title = %s,newspaper_text = %s,
newspaper_authors = %s,newspaper_summary = %s,newspaper_keywords = %s,
boilerpipe_article = %s,boilerpipe_articlesentences = %s,boilerpipe_keepeverything = %s,
boilerpipe_largestcontent = %s,boilerpipe_numwordsrules = %s,boilerpipe_canola = %s,
up_time = now(),count=count+1 """
     
    for link in headlines_links:
        #长度小于35的url一般都不是新闻连接
        if link is None or len(link) <= 35:
            continue
        try:
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
                       boilerpipe_numwordsrules,boilerpipe_canola,   \
                       code, newspaper_title,newspaper_text, newspaper_authors,\
                       newspaper_summary,newspaper_keywords,boilerpipe_article,boilerpipe_articlesentences,boilerpipe_keepeverything,\
                       boilerpipe_largestcontent,boilerpipe_numwordsrules,boilerpipe_canola)
            cursor.execute(in_sql,content)
            
            
        except:
            logger.error("crawl_page failed ,Error:%s" % traceback.format_exc())
                
    
            

    
def get_comps():
    '''从文件中获取所有的上市公司名单'''
    code_names = []
    try:
        code_names_file = '../data/code_name_vol.lst'
        path = os.path.realpath(__file__)
        path = os.path.dirname(path)
        lspath = os.path.split(path)
        if lspath[0] and lspath[1]:
            code_names_file = lspath[0] + '/data/code_name_vol.lst'
        
        
        for code_name in open(code_names_file):
            if code_name and len(code_name) > 0:
                cols = code_name.split('\t')
                if cols and len(cols) > 1:
                    code_names.append(cols[0])
    except:
        logger.error("crawl_page failed ,Error:%s" % traceback.format_exc())     
    return code_names

def go_yahoo():
    '''抓取雅虎的所有公司'''
    baseCrawl = BaseCrawl()
    baseExtract = BaseExtract()
    
    cxn_db = None
    try:
        cxn_db = get_cxn_db()
        cur_db = cxn_db.cursor()
        in_sql = """ insert into yahoo_comps (code_name,crawl_time,return_type,add_time,count)
values (%s,now(),%s,now(),1)
on duplicate key update crawl_time = now(),return_type = %s,count=count+1 """
        code_names = get_comps()
        logger.info("number of companys is %d"%len(code_names))
        for code in code_names:
            logger.debug("crawl %s, start............ "%code)
            status,headlines_links = go_crawl_headlines(code,baseCrawl,baseExtract)
            cur_db.execute(in_sql,(code,status,status))
            if status == 0 and len(headlines_links) > 0:
                extract_headlines_news(code,headlines_links,cur_db)
            logger.debug("crawl %s, end.............. "%code)
    
    except:
        logger.error("crawl_page failed ,Error:%s" % traceback.format_exc()) 
    finally:
        if cxn_db:
            cxn_db.close() 
    

if __name__ == '__main__':
#     logger.info("crawl yahoo_headlines start......")
#     go_yahoo()
#     logger.info("crawl yahoo_headlines end......")
    pass
    