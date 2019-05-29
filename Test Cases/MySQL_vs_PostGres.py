from MySQL_Server_Testing import MySQL_Server_Testing
from Postgre_Server_Testing import Postgre_Server_Testing
import pytest
import pandas as pd

mysql = MySQL_Server_Testing()
postgres = Postgre_Server_Testing()

@pytest.mark.connection
def test_1_connection_with_source():
    mysql.connection_String(1)

@pytest.mark.connection
def test_2_connection_with_destination():
    postgres.connection_String(1)

@pytest.mark.comparison
def comparison_With_counts():
    source = mysql.create_list(1)
    destination = postgres.create_list(1)
    print("MySQL List is :")
    print(source)
    print('\n')
    print("PostGres List is: ")
    print(destination)

    if len(source) != len(destination):
        diff_results = [x for x in source if x not in destination]
        print("Length is not Same, as "+str(len(diff_results))+" queries are missing, following Queries are missing")
        df = pd.DataFrame(diff_results)
        print(df)
    else:
        print("Number of Queries are Same")

def comparison_with_data():
    source = mysql.create_list(1)
    destination = postgres.create_list(1)

#comparison_With_counts()
