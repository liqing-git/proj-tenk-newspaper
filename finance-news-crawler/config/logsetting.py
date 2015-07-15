#coding=utf-8
'''
日志类，将日志定向到文件和控制台
Created on 2015-06-12

@author: liqing01
'''
import logging
import os

def initlog(level):
    logfile = '../data/logs/result.log'
    path = os.path.realpath(__file__)
    path = os.path.dirname(path)
    lspath = os.path.split(path)
    if lspath[0] and lspath[1]:
        logfile = lspath[0] + '/data/logs/result.log'
    logger = logging.getLogger()
    '''日志文件'''
    fh = logging.FileHandler(logfile)
    '''控制台'''
    console = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    fh.setFormatter(formatter)
    console.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(console)
    logger.setLevel(level)
    return logger

logger = initlog(logging.NOTSET)

if __name__ == '__main__':
    logger.debug('debug')
    logger.info('info')
    logger.error('error')
    logger.fatal('fatal')
