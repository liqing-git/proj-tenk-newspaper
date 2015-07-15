# -*- coding: utf-8 -*-
'''
Created on 2015年6月12日

@author: liqing
'''

import urllib, urllib2, sys, cookielib, re, os, json

if __name__ == '__main__':
    cj = cookielib.CookieJar()
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    r = opener.open('https://www.baidu.com/')
    print r.read()
    r.close()
    pass
    