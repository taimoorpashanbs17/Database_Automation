from MySQL_Server_Testing import MySQL_Server_Testing
import pytest

mysql = MySQL_Server_Testing()

@pytest.mark.connection
def test_1_connection_with_source():
    mysql.connection_String(1)

@pytest.mark.connection
def test_2_connection_with_source():
    mysql.connection_String(2)

@pytest.mark.execute_query
def test_3_running_query_from_source():
    mysql.running_query(1, "SELECT * FROM city")

@pytest.mark.execute_query
def test_4_running_query_from_destination():
    mysql.running_query(2, "SELECT * FROM city")

@pytest.mark.list_comparison
def test_5_running_queries_from_list():
    mysql.running_queries_from_list()

@pytest.mark.comparison
def test_6_checking_comaprison():
    mysql.comparing_contsraints('SHOW CREATE TABLE information_schema.tables', 'Contsrainst')

@pytest.mark.comparison
def test_7_comparing_schemas():
    mysql.comparing_contsraints('Show SCHEMAS', 'Schemas')



