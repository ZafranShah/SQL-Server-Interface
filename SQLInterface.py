# -*- coding: utf-8 -*-
"""
Created on Wed May 15 10:06:01 2019

@author: zhshah
"""

from __future__ import division
import pyodbc
import json 

data=[]



def LoadParameters(PathofJsonFile):
    with open(PathofJsonFile) as paramfile:
        data=json.load(paramfile)
        return data

class SqlInterface():
    
    def __init__(self, sqldriver, serverName, dataBaseName,  userName, password, tableName, columnName):    
        
        self.drivername = sqldriver
        self.servername= serverName
        self.databasename= dataBaseName
        self.uid= userName
        self.pwd=password
        self.table= tableName
        self.columnname=columnName

    
    def Sqlconnection(self):
        connection = pyodbc.connect('Driver={'+str(self.drivername)+'};'
                                         'Server='+str(self.servername)+';'
                                         'Database='+str(self.databasename)+';'
                                         'UID='+str(self.uid)+';'
                                         'PWD='+str(self.pwd)+';'
                                         'Trusted_Connection=yes;')
        print (connection)
        cursor= connection.cursor()
        print ('Connection established with the SQL Server')
        return cursor
    
    def AccessingData(self):
        cursor= self.Sqlconnection()
    
        sql="Select features from "+str(self.table)+" where status = 'T'";
        print ('Query sent to the Sql Server', sql)
        try:
            cursor.execute(sql)
            row= cursor.fetchone()
            for row in cursor:
                data.append((row[0], row[1], row[2],row[3], row[4], row[5], row[6], row[7], row[8]))
        except Exception as ProgrammingError:
           print (ProgrammingError)
        finally:
           print ('Successfully Loaded the data from Sql')
           
           
    def AlterTable(self):
        cursor= self.Sqlconnection()
        
        sql="IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE  TABLE_NAME = '"+str(self.table)+"' AND COLUMN_NAME ='" +str(self.columnname)+"') BEGIN ALTER TABLE " +str(self.table)+" ADD "+str(self.columnname)+" int null END;"
        try:
            cursor.execute(sql)
            cursor.commit()
        except Exception as ProgrammingError:
            print (ProgrammingError)
        finally:
            print ('New Column is added in to the database', self.columnname)
            
            
    def UpdateTable(self,value):
        
        cursor= self.Sqlconnection()
        sql="update "+str(self.table)+" set "+ str(self.columnname)+ "="+ str(value)
        print (sql)
        try:
            cursor.execute(sql)
            cursor.commit()
        except Exception as ProgrammingError:
            print (ProgrammingError)




if __name__ == "__main__":
    JsonParameters=LoadParameters("..\\User\\config.json")
    SQL_Driver=JsonParameters['DataBaseDriver']
    DataBase_Name=JsonParameters['DataBaseName']
    Database_Server=JsonParameters['DatabaseServerName']
    username= JsonParameters['UserName']
    pswd=JsonParameters['password']
    Table_Name=JsonParameters['Table']
    column_Name = JsonParameters['ColumnName']
    obj= SqlInterface(SQL_Driver, Database_Server,DataBase_Name, username, pswd, Table_Name, column_Name)
    obj.AlterTable()
    obj.UpdateTable(101)
else: 
    print ('exported into this module')
 