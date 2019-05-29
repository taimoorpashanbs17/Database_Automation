from MySQL_Server_Testing import MySQL_Server_Testing
import unittest

mysql = MySQL_Server_Testing()
class MySQL_unittest_TestCases(unittest.TestCase):

    def test_1_connection_with_source(self):
        mysql.connection_String(1)

    def test_2_connection_with_source(self):
        mysql.connection_String(2)

    def test_3_running_query_from_source(self):
        mysql.running_query(1, "SELECT * FROM actor")

    def test_4_running_query_from_destination(self):
        mysql.running_query(2, "SELECT * FROM actor")

    def test_5_running_queries_from_list(self):
        mysql.running_queries_from_list()

    def test_6_checking_comaprison(self):
        mysql.comparing_contsraints('SHOW CREATE TABLE information_schema.tables', 'Contsrainst')

    def test_7_comparing_schemas(self):
        mysql.comparing_contsraints('Show SCHEMAS', 'Schemas')

if __name__ == "__main__":
    unittest.main()
