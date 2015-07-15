# -*- coding: utf-8 -*-
'''
Created on 2015年6月12日

基本的抓取和抽取类 
@author: liqing
'''

import urllib
import urllib2
from bs4 import BeautifulSoup
from config.setencoding import set_encoding
set_encoding()
from config.logsetting import logger
import traceback

#模拟浏览器
Headers = {'User-Agent':
           'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6',
           }

class RedirctHandler(urllib2.HTTPRedirectHandler):
    """docstring for RedirctHandler"""
    def http_error_301(self, req, fp, code, msg, headers):
        pass
    def http_error_302(self, req, fp, code, msg, headers):
        pass

class BaseCrawl(object):
    '''页面抓取基类'''
    def __getheaders(self, referer):
        '''模拟header过程
        @param referer: 构造前链
        '''
        headers = Headers
        if referer:
            headers['Referer'] = referer
        return headers
    def __get_req(self, url, data=None, referer=None):
        '''构造http请求对象'''
        headers = self.__getheaders(referer)
        if data:
            data = urllib.urlencode(data)
        return urllib2.Request(url=url,
                               data=data,
                               headers=headers)
    def crawl_page(self, req_url, req_data=None , req_referer=None):
        '''页面抓取的公共方法'''
        response = None
        try:
#             req = self.__get_req(req_url, req_data, req_referer)
            debug_handler = urllib2.HTTPHandler(debuglevel = 0)
            opener = urllib2.build_opener(debug_handler, RedirctHandler)
            headers = Headers
            if req_referer:
                headers['Referer'] = req_referer
            opener.handlers.append(headers)
            
            response = opener.open(req_url,timeout=20)
#             response = urllib2.urlopen(req,timeout=1000)
            html = ''
            status = response.getcode()
            
            if status == 200:
                html = response.read()
            
            return status,html
        except urllib2.URLError, e:
            if hasattr(e, 'code'):
                logger.error("URLError, code is :%s" % e.code)
                return  e.code,''
            elif hasattr(e, 'reason'):
                logger.error("URLError, code is :%s" % e.reason)
                return e.reason,''
        except:
            logger.error("crawl_page failed ,Error:%s" % traceback.format_exc())
            return -100,''
        finally:
            if response:
                response.close()
        
class BaseExtract(object):
    '''页面抽取基类'''
    def get_bs(self, markup, parser='lxml', page_encoding=None):
        '''获取定制的BS
        @param markup: 网页内容
        @param parser: 解析器，替换html.parser默认使用 lxml解析,very fast
        解析器有：lxml>html.parser>html5lib
        @param page_encoding: 网页编码
        '''
        return BeautifulSoup(markup, features=parser, from_encoding=page_encoding)

        
        