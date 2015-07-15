# -*- coding: utf-8 -*-
import MySQLdb,traceback
from config.logsetting import logger

from config.dbsetting import DB_HOST,DB_USER,DB_PASSWD,DB_NAME,DB_PORT

cxn_db = MySQLdb.connect(host=DB_HOST,user=DB_USER,passwd=DB_PASSWD,
                         db=DB_NAME,port=DB_PORT,charset='UTF8')

def get_cxn_db():
    '''获取数据库的连接'''
    try:
        return MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME, port=DB_PORT, charset='UTF8')
    except:
        logger.error('create mysql connect error' + ',Except: ' + traceback.format_exc())
        return None
