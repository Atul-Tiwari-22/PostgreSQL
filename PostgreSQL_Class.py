import pandas as pd
import psycopg2
import sys

class PostgreSQL_hunt:

    # for loging the Activicity of the Process
    logs = []

    def Update_data(self,table,column,new_value,WhereCondtion):
        # updating Data
        try:
            self.Custom_Query("UPDATE {} SET {} = '{}' WHERE {};".format(table,column,new_value,WhereCondtion))
        except:
            print("Exception in updating Data")
            self.logs.append("Exception in updating Data")
            return -1
          

    def Delete_data(self,table,WhereCondtion):
        try:
            self.Custom_Query('DELETE FROM {} WHERE {}'.format(table,WhereCondtion))
            print("Deleted successfully")
        except:
            print("Exception in Deleting Data")
            self.logs.append("Exception in Deleting Data")
            return -1
    
    def GetallTableData(self,tablename):
        # rechive all the data in the data frame
        try:
            data  = self.GetData_DF('select * from {}'.format(tablename))
        except (Exception, psycopg2.DatabaseError) as error:
            data = []
            print('Data not fetched error in the query',error)
            self.logs.append(' Exception Data not fetched error in the query {}'.format(error))
        return data

    def Custom_Query(self,query):

        self.Database_connection()

        try:
            # create a cursor
            cur = self.connection.cursor()
            # execute a statement
            print(query)
            cur.execute(query)
            # display the PostgreSQL database server version
            print(cur.description)
            #output = cur.fetchall()
            # close the communication with the PostgreSQL

            cur.close()
            self.connection.commit()
            return 0

        except(Exception, psycopg2.DatabaseError) as error:
            
            #saving the log and printing the error
            self.logs.append("Error at tessting currsor"+str(error))
            print(error , " AT Error at TEsting connection",error)
            self.connection.rollback()
            self.close_connection()
            exit(1)
            return -1
        finally:
            self.close_connection()
    
    def Check_df(self,ch_DataFrame,table_name):
        
        self.Database_connection()
        try:
            # create a cursor
            cur = self.connection.cursor()
            # execute a statement
            statement = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'".format(table_name)
            cur.execute(statement)
            # display the PostgreSQL database server version
            output = cur.fetchall()
            
            columns = list(map(lambda x: x[0],output))

            columns.sort()
            columns_dataframe = list(ch_DataFrame.keys())
            columns_dataframe.sort()

            # close the communication with the PostgreSQL
            cur.close()

            if columns == columns_dataframe:
                return True
            else:
                print("Dataframe ",columns_dataframe)
                print("Database ",columns)
                print(statement)
                return False

        except(Exception, psycopg2.DatabaseError) as error:
            #saving the log and printing the error
            self.logs.append("Error at tessting currsor"+str(error))
            print(error , " AT Error at TEsting connection")
            self.close_connection()
            exit(1)

        finally:
            self.close_connection()


    def Test_connection(self):
        # testing connection code
        self.Database_connection()
        try:
            # create a cursor
            cur = self.connection.cursor()
            
            # execute a statement
            print(' Connection Successfull PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)
        
            # close the communication with the PostgreSQL
            cur.close()
            print('Running')
        except(Exception, psycopg2.DatabaseError) as error:
            #saving the log and printing the error
            self.logs.append("Error at tessting currsor"+str(error))
            print(error , " AT Error at TEsting connection")
            self.close_connection()
            exit(1)

        finally:
            self.close_connection()

    def insert_data_all(self,Data_DataFrame, table_name):
        self.Database_connection()
        try:
            cur = self.connection.cursor()

            #self.connection.autocommit = True
            # get columns from df
            columns = '({})'.format(','.join(Data_DataFrame.keys()))

            #create sql formating for multiple row insertion
            row_data = [list(dic) for row, dic in Data_DataFrame.iterrows()]

            row_data = ['{}'.format(tuple(map(lambda x:str(x),i))) for i in row_data]

            row = '{}'.format(','.join(row_data))

            row= row.replace("'nan'",'NULL').replace("' '",'NULL')
            
            query = "insert into {}{} values{}".format(table_name,columns,row)
            # execute a statement
            cur.execute(query)

            if cur.description == None:
                print("inserted data rows = {}".format(len(row_data)))
                self.logs.append("inserted data rows = {}".format(len(row_data)))
        
            # close the communication with the PostgreSQL
            self.connection.commit()
            cur.close()
        except(Exception, psycopg2.DatabaseError) as error:
            #saving the log and printing the error
            #print(query)
            self.logs.append("Error at Getdata "+str(error))
            print(error , " AT Error at currsor",error)
            self.connection.rollback()
            return -1
        finally :
            self.close_connection()

    def insert_data(self,Data_dictonary, table_name):
        
        self.Database_connection()
        try:
            cur = self.connection.cursor()

            # formating the SQL standards            
            columns = '({})'.format(','.join(Data_dictonary.keys()))
            statement = "insert into {}{} values{}".format(table_name,columns,tuple(Data_dictonary.values()))
            statement=statement.replace("'nan'",'NULL').replace("' '",'NULL')
            # execute a statement
            cur.execute(statement)

            if cur.description == None:
                print("inserted data rows = 1")
                self.logs.append("inserted data rows = 1")
        
            # close the communication with the PostgreSQL
            self.connection.commit()
            cur.close()
        except(Exception, psycopg2.DatabaseError) as error:
            #saving the log and printing the error

            self.logs.append("Error at Getdata "+str(error))
            print(error , " AT Error at currsor")
            self.connection.rollback()
            self.close_connection()
            return -1
        finally :
            self.close_connection()       
        
    
    def GetData_DF(self, Query):
        # connect to the database create connection
        self.Database_connection()

        # create a cursor
        try:
            cur = self.connection.cursor()
            
            # execute a statement
            try:
                cur.execute(Query)

            except(Exception, psycopg2.DatabaseError) as error:
                #saving the log and printing the error
                self.logs.append("Error at Getdata "+str(error))
                print(error , " AT Error at Quary")
                cur.colse()
                self.close_connection()
                return -1

            # Save all data from the PostgreSQL database
            rechived_data = cur.fetchall()

            try:
                # get all the column names in the result
                Column_names = [i[0] for i in cur.description]

            except(Exception, psycopg2.DatabaseError) as error:
                #saving the log and printing the error
                self.logs.append("Error at Getting columns "+str(error))
                print(error , "Error at Getting columns")
                cur.colse()
                self.close_connection()
                return -1
        
            # close the communication with the PostgreSQL
            cur.close()

            # returning the dataframe object
            return pd.DataFrame(rechived_data,columns=Column_names)
        except(Exception, psycopg2.DatabaseError) as error:
            #saving the log and printing the error

            self.logs.append("Error at Getdata "+str(error))
            print(error , " AT Error at currsor")
            self.close_connection()
            return -1
        finally :
            self.close_connection()


    def close_connection(self):
        try:
            #closing the opened connection
            self.connection.close()
        except (Exception, psycopg2.DatabaseError) as error:
            #saving the log and printing the error
            self.logs.append("Error at Colsing "+str(error))
            print(error , " AT Error at closing Connection")
            sys.exit()


    def Database_connection(self):
        """ Connect to the PostgreSQL database server a"""
        self.connection = None
        try:
            # read connection parameters
            self.logs.append('Connecting to the PostgreSQL database...')
            self.connection = psycopg2.connect(host=self.host,
                                               database=self.database,
                                               user=self.user,
                                               password=self.password)
            
        except (Exception, psycopg2.errors.ConnectionException) as error:
            # checking the connection error
            print("Check your COnnection String ")
            self.logs.append("psycopg2.errors.ConnectionException  Occured ")
            exit(1)
        
        except (Exception, psycopg2.DatabaseError) as error:
            #saving the log and printing the error
            self.logs.append("Error at Colsing "+str(error))
            print(error , " AT Error at Connection function")
            exit(1)



    def __init__(self):

        # Deftine all the Database Acess Code

        self.host="hunt.cpi0ssunxeuy.ap-south-1.rds.amazonaws.com"
        self.database="postgres"
        self.user="postgres"
        self.password="amazonhunt$4"

        self.Test_connection()