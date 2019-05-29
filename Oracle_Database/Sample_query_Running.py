import cx_Oracle
import pandas as pd
import os
from openpyxl import load_workbook


class All_Methods():

    def connection_string(self, connection_number):
        excel_sheet = os.getcwd() + '/Docs/data_entry.xlsx'
        actual_path = os.path.abspath(excel_sheet)
        wb = load_workbook(actual_path)
        sheet_name = wb['Info']
        user_name = sheet_name['A'+''+format(connection_number)].value
        password = sheet_name['B'+''+format(connection_number)].value
        host_name = sheet_name['C'+''+format(connection_number)].value
        sid = sheet_name['D'+''+format(connection_number)].value
        port = sheet_name['E'+''+format(connection_number)].value
        self.dsn_tns = cx_Oracle.makedsn(host_name, port, sid)
        self.connection = cx_Oracle.connect(user_name, password, self.dsn_tns)
        con_str = (user_name + "@" + host_name + "/" + sid+': '+str(port))
        print("connection establised with "+str(con_str))
        print("Connection is using "+str(self.connection.version)+" version.")
        return self.connection, self.dsn_tns

    def running_query(self, query):
        connect = self.connection_string(3)
        sql_query = query
        query = pd.read_sql(sql_query, con=self.connection)
        print(query)
        return query

    def getting_query_results(self, connection_number, query):
        connect = self.connection_string(connection_number)
        sql_query = query
        query = pd.read_sql(sql_query, con=self.connection)
        return query

    def comparing_queries(self, query1, query2):
        run_query1 = self.getting_query_results(1, query1)
        run_query2 = self.getting_query_results(1, query2)
        if run_query1.equals(run_query2):
            print("Data are Equal")
        else:
            difference = pd.concat([run_query1, run_query2]).drop_duplicates(keep=False)
            file_path = os.path.abspath(os.getcwd()+'/Pandas_Doc/difference.xlsx')
            if os.path.exists(file_path):
                os.remove(file_path)
                writer = pd.ExcelWriter(file_path)
                difference.to_excel(writer, sheet_name='Difference')
                writer.save()
                print("Sheet created on following path ")
                print(file_path)
            else:
                writer = pd.ExcelWriter(file_path)
                difference.to_excel(writer, sheet_name='Difference')
                writer.save()
                print("Sheet created on following path ")
                print(file_path)
            raise AssertionError("Data is different")

    def duplicate_results(self, query):
        connect = self.connection_string(1)
        run_query = self.getting_query_results(1, query)
        duplicates_bool = run_query.duplicated(keep='first')
        duplicates = run_query.loc[duplicates_bool == True]
        if duplicates is None:
            print("There are no Duplicates")
        else:
            print(duplicates)
            file_path = os.path.abspath(os.getcwd() + '/Pandas_Doc/Duplicates.xlsx')
            if os.path.exists(file_path):
                os.remove(file_path)
                writer = pd.ExcelWriter(file_path)
                duplicates.to_excel(writer, sheet_name='Duplicates')
                writer.save()
            else:
                writer = pd.ExcelWriter(file_path)
                duplicates.to_excel(writer, sheet_name='Difference')
                writer.save()
            raise AssertionError("There are Duplicates, Sheet created on following path "+str(file_path))



Method = All_Methods()
Method.duplicate_results("SELECT * FROM QATEST.BST_CMPD_CONSTANT_GENERAL")
