import xlrd
import os
from openpyxl import load_workbook
import cx_Oracle
import pandas as pd


class compaison():
    def getting_queries(self):
        queries_path = os.path.abspath(os.getcwd()+'/queries_check.xlsx')
        wb = xlrd.open_workbook(queries_path)
        sheet = wb.sheet_by_index(0)
        sheet.cell_value(0, 0)
        tables = []
        for i in range(sheet.nrows):
            tables.append(sheet.cell_value(i, 0))
        queries = ["SELECT * FROM " + e + "" for e in tables]
        print(queries)
        self.queries = queries
        return self.queries

    def connection_string(self, connection_number):
        excel_sheet = os.getcwd() + '/Docs/data_entry.xlsx'
        actual_path = os.path.abspath(excel_sheet)
        wb = load_workbook(actual_path)
        sheet_name = wb['Info']
        user_name = sheet_name['A' + '' + format(connection_number)].value
        password = sheet_name['B' + '' + format(connection_number)].value
        host_name = sheet_name['C' + '' + format(connection_number)].value
        sid = sheet_name['D' + '' + format(connection_number)].value
        port = sheet_name['E' + '' + format(connection_number)].value
        self.dsn_tns = cx_Oracle.makedsn(host_name, port, sid)
        try:
            self.connection = cx_Oracle.connect(user_name, password, self.dsn_tns)
            con_str = (user_name + "/" + "@" + host_name + "/" + sid + ': ' + str(port))
            print("connection establised with " + str(con_str))
            print("Connection is using " + str(self.connection.version) + " version.")
        except ConnectionError as error_message:
            raise AssertionError("Connection couldn't be build due to " + str(error_message))
        return self.connection, self.dsn_tns

    def running_queries_list(self, connection_number):
        connect1 = self.connection_string(connection_number)
        get_list = self.getting_queries()
        queries = self.queries
        for q in queries:
            df = pd.read_sql_query(q, self.connection)
            print(df)
        return df

    def getting_difference(self):
        source_result = self.running_queries_list(2)
        target_results = self.running_queries_list(3)
        difference = pd.concat([source_result, target_results]).drop_duplicates(keep=False)
        if source_result.equals(target_results):
            print("Data is Same")
        else:
            print(difference)
            raise AssertionError ("Data is not Same")

compare = compaison()
compare.getting_difference()
