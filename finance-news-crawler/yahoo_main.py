# -*- coding: utf-8 -*-
'''
Created on 2015年6月15日

抓取并抽取来源于yahoo的财经频道和每个公司频道的新闻
@author: liqing
'''
from config.logsetting import logger
from config.dbsetting import SOURCE_TYPE_YAHOO
from crawl.newsextracte import ExtracteNewsurls,go_newsextrac

class YahooExtracteNewsurls(ExtracteNewsurls):
    """
    对来源于yahoo财经频道和每个公司频道的进行新闻url抽取
    """
    def extracteUrls(self,baseExtract,html,is_finance_home):
        """提取新闻链接
        @param baseExtract: 封装的抽取基类
        @param html:网页内容 
        @param is_finance_home: 是否是finance_home，可能需要单独处理
        """
        news_links = []
        beautifulSoup = baseExtract.get_bs(html)
        if is_finance_home :
            content = beautifulSoup
        else: 
            #可视化模板抽取新闻的div中得链接
            content = beautifulSoup.find('div', {'class':'mod yfi_quote_headline withsky'})
        if content:
            for link in content.findAll('a'):
                news_links.append(link.get('href'))
        return news_links
        
        

def go_yahoo():
    logger.info("crawl yahoo finance start......")
    source_type = SOURCE_TYPE_YAHOO
    code_names_file = 'code_name_vol.lst'
    url_prefix = 'http://finance.yahoo.com/q/h?s='
    req_referer = 'http://finance.yahoo.com/'
    finance_homes = ['http://finance.yahoo.com/']
    extracteNewsurls = YahooExtracteNewsurls()
    
    go_newsextrac(source_type,code_names_file,url_prefix,req_referer,finance_homes,extracteNewsurls)
    
    logger.info("crawl yahoo finance end......")

if __name__ == '__main__':
    go_yahoo()
    pass
