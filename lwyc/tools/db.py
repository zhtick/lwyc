'''
通用的程序
'''
import pymysql

def readdb(cmd, server= "10.24.18.101", user= "root", passwd= "lcwkd123", db= "zfdb"):
    connection = pymysql.connect(server, user, passwd, db, charset="GBK")
    cursor = connection.cursor()
    cursor.execute(cmd)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results
