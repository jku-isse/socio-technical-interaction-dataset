# -*- coding: utf-8 -*-
"""
Created on Mon May 20 13:54:09 2019

@author: Usman
"""

import mysql.connector

"""Provide db, user, and password details of your local mysql server"""

connection  = mysql.connector.connect(
            host="localhost",
            user="",
            password="",
            auth_plugin='mysql_native_password',
            database = ''
)
def getDBConn():
    return connection