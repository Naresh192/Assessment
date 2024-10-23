import unittest
from etl.connection import connect_to_db
from etl.extract import extract_data
from etl.load import load_data
import sqlite3
import pandas as pd
from datetime import datetime

class TestETL(unittest.TestCase):
    def setUp(self):
        self.db_name = 'hospital.db'
        self.master_file = r'data/master.csv'
        self.extracted_data = pd.DataFrame({
            'Customer_Name': ['Alex'],
            'Customer_Id': '123457',
            'Open_Date': [datetime.strptime('20101012', '%Y%m%d')],
            'Last_Consulted_Date': [datetime.strptime('20121013', '%Y%m%d')],
            'Vaccination_Id': ['MVD'],
            'Dr_Name': ['Paul'],
            'State': ['SA'],
            'Country': ['USA'],
            'DOB': [datetime.strptime('19870603', '%Y%m%d')],
            'Is_Active': ['A']
        })
        self.incremental_column='Last_Consulted_Date'
        self.group_by='Country'
        
    def test_db_conn(self) :
        try:
            connect = connect_to_db(self.db_name)
            connect.close()
            connection_successful = True
        except sqlite3.Error :
            connection_successful = False
        
        self.assertTrue(connection_successful)
    def test_extract(self) :
        data = extract_data(self.master_file)
        for files,schema in data :
            pass
            #pd.testing.assert_frame_equal(files, self.extracted_data)
            #self.assertEqual(schema, self.schema)
        
    def test_load(self) :
            conn = connect_to_db(self.db_name)
            data = extract_data(self.master_file)
            for files,schema in data :
                load_data(files,conn,schema,self.incremental_column,self.group_by)
            cursor = conn.cursor()
            cursor.execute("SELECT Customer_Name FROM USA WHERE Customer_Id=?", ('123457',))
            result = cursor.fetchone()
            self.assertIsNotNone(result)
            self.assertEqual(result[0], 'Alex')
            conn.close()

        
   
        
if __name__ == '__main__':
    unittest.main()
    

