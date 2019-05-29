import pytds
import pandas as pd
from openpyxl import load_workbook
import os

class SQL_Server_Testing:

    def connection_String(self, connection_number):
        excel_sheet = os.getcwd() + '/Docs/info_data.xlsx'
        actual_path = os.path.abspath(excel_sheet)
        wb = load_workbook(actual_path)
        sheet_name = wb['sql_server']
        server_name = sheet_name['A'+''+format(connection_number)].value
        username = sheet_name['B'+''+format(connection_number)].value
        password = sheet_name['C'+''+format(connection_number)].value
        if connection_number ==1:
            Database_name = "AdventureWorks2014"
            database = "Source"
        elif connection_number ==2:
            Database_name = "AdventureWorks2014"
            database = "Destination"
        else:
            return False
        conn = pytds.connect(server_name, Database_name, username, password)
        try:
            print(database+" is connected with following address with ")
            print(server_name+"/"+username)
            print("The version of "+database+" side is "+str(conn.product_version))
        except:
            raise ConnectionError
        self.conn = conn
        return self.conn

    def running_query(self, connection_number, query):
        sql_connect = self.connection_String(connection_number)
        df = pd.read_sql(query, con = self.conn)
        print(df)
        return df

    def create_list(self, connection_number, query_type):
        sql_connect = self.connection_String(connection_number)
        if connection_number ==1:
            tables_sheet = os.path.abspath(os.getcwd()+'/Docs/SQLServer_source_table_sheet.xlsx')
            sheet = "Source_Tables"
        elif connection_number ==2:
            tables_sheet = os.path.abspath(os.getcwd()+'/Docs/SQLServerDestination_table_sheet.xlsx')
            sheet = "Destination_Tables"
        else:
            return False
        query = "SELECT TOP 20 CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) as Table_FullName FROM AdventureWorks2014.INFORMATION_SCHEMA.TABLES"
        if os.path.exists(tables_sheet):
            os.remove(tables_sheet)
            writer = pd.ExcelWriter(tables_sheet)
            df = pd.read_sql(query, con= self.conn)
            df.to_excel(writer, sheet_name=sheet)
            writer.save()
        else:
            writer = pd.ExcelWriter(tables_sheet)
            df = pd.read_sql(query, con= self.conn)
            df.to_excel(writer, sheet_name=sheet)
            writer.save()
        access_sheet = pd.read_excel(tables_sheet, sheet_name= sheet)
        table_list = []
        for i in access_sheet.index:
            table_list.append(df['Table_FullName'][i])
        queries = ["SELECT TOP 50 * FROM " + e + "" for e in table_list]
        print("Number of Tables are :"+str(len(queries)))
        self.queries = queries
        return self.queries

    def running_queries_from_list(self, connection_number):
        getting_list = self.create_list(connection_number)
        if connection_number ==1:
            query_sheet = os.path.abspath(os.getcwd()+'/Docs/SQLServer_Source_query_results.xlsx')
            db = 'Source'
        elif connection_number ==2:
            query_sheet = os.path.abspath(os.getcwd()+'/Docs/SQLServer_Destination_query_results.xlsx')
            db = 'Destination'
        else:
            return False
        if os.path.exists(query_sheet):
            os.remove(query_sheet)
            writer = pd.ExcelWriter(query_sheet)
            for q in self.queries:
                df = pd.read_sql_query(q, self.conn)
                print(df)
                print(df.dtypes)
                dataFrame =df.astype(str)
                print (dataFrame.dtypes)
                dataFrame.to_excel(writer, sheet_name=str(self.queries.index(q) + 1), encoding='default')
            writer.save()
            print("Results are saved on following path: ")
            print('\n')
            print(query_sheet)
        else:
            writer = pd.ExcelWriter(query_sheet)
            for q in self.queries:
                df = pd.read_sql_query(q, self.conn)
                print(df)
                print(df.dtypes)
                dataFrame = df.astype(str)
                print(dataFrame.dtypes)
                dataFrame.to_excel(writer, sheet_name=str(self.queries.index(q) + 1), encoding='default')
            writer.save()
            print("Results of "+db+" Database are saved on following path: ")
            print('\n')
            print(query_sheet)

sql_connect = SQL_Server_Testing()
sql_connect.running_queries_from_list(1)
print('\n')
sql_connect.running_queries_from_list(2)









