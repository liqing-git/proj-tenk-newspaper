# -*- coding: utf-8 -*-
'''
Created on 2015年6月18日

抓取并抽取来源于google的财经频道和每个公司频道的新闻


@author: liqing
'''
from config.logsetting import logger
from config.dbsetting import SOURCE_TYPE_GOOGLE
from crawl.newsextracte import ExtracteNewsurls,go_newsextrac

class GoogleExtracteNewsurls(ExtracteNewsurls):
    """
    对来源于google财经频道和每个公司频道的进行新闻url抽取
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
            content = beautifulSoup.find('div', {'class':'g-c sfe-break-right'})
        if content:
            for link in content.findAll('a'):
                news_links.append(link.get('href'))
        return news_links

def go_google():
    logger.info("crawl google finance start......")
    source_type = SOURCE_TYPE_GOOGLE
    code_names_file = 'code_name_vol.lst'
    url_prefix = 'https://www.google.com/finance/company_news?q='
    req_referer = 'https://www.google.com/finance'
    finance_home = ['https://www.google.com/finance','https://news.google.com/news/section?ned=us&topic=b']
    extracteNewsurls = GoogleExtracteNewsurls()
    
    go_newsextrac(source_type,code_names_file,url_prefix,req_referer,finance_home,extracteNewsurls)
    
    logger.info("crawl google finance end......")

if __name__ == '__main__':
    go_google()
    pass
