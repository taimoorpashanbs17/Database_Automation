import cx_Oracle

import pandas as pd
from pandas import ExcelWriter
import xlrd
import os
from openpyxl import load_workbook
import collections


class All_Methods():

    def connection_string(self):
        excel_sheet = os.getcwd() + '/Docs/data_entry.xlsx'
        actual_path = os.path.abspath(excel_sheet)
        wb = load_workbook(actual_path)
        sheet_name = wb['Info']
        user_name = sheet_name['A1'].value
        password = sheet_name['B1'].value
        host_name = sheet_name['C1'].value
        sid = sheet_name['D1'].value
        port = sheet_name['E1'].value
        con_str = (user_name + "/" + password + "@" + host_name + "/" + sid)
        self.con = con_str
        return self.con

    def report_writing(self, results, file_path):
        df = pd.DataFrame(results)
        path = os.getcwd() + '\\' + file_path
        if os.path.exists(os.path.abspath(path)):
            os.remove(os.path.abspath(path))
            writer = ExcelWriter(os.path.abspath(path))
            df.to_excel(writer, 'Sheet1', index=False)
            writer.save()
            print("Report of the Missing Matches can be found from the following link: " + '\n' + format(path))
        else:
            writer = ExcelWriter(os.path.abspath(path))
            df.to_excel(writer, 'Sheet1', index=False)
            writer.save()
            print("Report of the Missing Matches can be found from the following link: " + '\n' + format(path))

        return path

    def version(self):
        connection = self.connection_string()
        con = cx_Oracle.connect(self.con)
        print("Version Using is " + con.version)


    def query(self, query):
        connection = self.connection_string()
        con = cx_Oracle.connect(self.con)
        cur = con.cursor()
        cur.execute(query)
        for result in cur:
            print(result)
        cur.close()
        con.close()

    def query_diff(self, query1, query2):
        connection = self.connection_string()
        con = cx_Oracle.connect(self.con)

        cur = con.cursor()
        cur.execute(query1)

        result1 = cur.fetchall()
        print("Total Number of Records in 1st Query are " + str(len(result1)))

        cur2 = con.cursor()
        cur2.execute(query2)

        result2 = cur2.fetchall()
        print("Total Number of Records in 2nd Query are " + str(len(result2)))
        global diffenece
        if len(result1) != len(result2):
            diff_results = (x for x in result1 if
                            x not in result2)
            print("Missing Results are stored on Excel Sheet")
            print("Report of the Missing Matches can be found from the following link: " + '\n' + format(difference_path))
            if len(result1) > len(result2):
                diffenece = len(result1) - len(result2)
                print("1st Query has more records than 2nd i.e " + str(diffenece) + " more Records.")
            else:
                diffenece = len(result2) - len(result1)
                print("2nd Query has more Records than 1st i.e. " + str(diffenece) + " more Records.")
            raise AssertionError(diffenece >= 1,
                                 "There is a Difference in Number " + str(
                                     diffenece) + " ,Thus Test Case is Failed.Report of the Missing Matches can be found from the following link: " + '\n' + format(
                                     ))
        else:
            print("There is no Difference in Length, they are same")


    def getting_results(self):
        queries_path = os.getcwd()+'/Docs/queries_check.xlsx'

        wb = xlrd.open_workbook(queries_path)
        sheet = wb.sheet_by_index(0)
        sheet.cell_value(0, 0)
        tables = []
        for i in range(sheet.nrows):
            tables.append(sheet.cell_value(i, 0))
        print("Number of Tables are " + str(len(tables)))
        queries = ["SELECT * FROM " + e + "" for e in tables]
        connection = self.connection_string()
        con = cx_Oracle.connect(self.con)
        cursor = con.cursor()
        que_results_path = os.getcwd()+'/Docs/Queries_results.xlsx'
        if os.path.exists(que_results_path):
            os.remove(que_results_path)
            writer = pd.ExcelWriter(que_results_path)
            for q in queries:
                df = pd.read_sql_query(q, con)
                df.to_excel(writer, sheet_name=str(queries.index(q) + 1))
            print("Results of all Tables are showing on following link: ")
            writer.save()
        else:
            writer = pd.ExcelWriter(que_results_path)
            for q in queries:
                df = pd.read_sql_query(q, con)
                df.to_excel(writer, sheet_name=str(queries.index(q) + 1))
            print("Results of all Tables are showing on following link: ")
            print(que_results_path)
            writer.save()


    def duplications(self):
        connection = self.connection_string()
        con = cx_Oracle.connect(self.con)
        query = "SELECT * FROM BST_CMPD_CONSTANT_GENERAL"

        cur = con.cursor()
        cur.execute(query)
        for result in cur:
            result = cur.fetchall()
        duplicates = [item for item, count in collections.Counter(result).items() if count > 1]
        df = pd.DataFrame(duplicates)
        print(df)
        path = os.getcwd() + '\\' + 'Duplicates.xlsx'
        print(path)
        if os.path.exists(os.path.abspath(path)):
            os.remove(os.path.abspath(path))
            writer = ExcelWriter(os.path.abspath(path))
            df.to_excel(writer, 'Sheet1', index=False)
            writer.save()
            print("Report of the Missing Matches can be found from the following link: " + '\n' + format(path))
        else:
            writer = ExcelWriter(os.path.abspath(path))
            df.to_excel(writer, 'Sheet1', index=False)
            writer.save()
        print("Report of the Missing Matches can be found from the following link: " + '\n' + format(path))
