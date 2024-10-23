from etl.connection import connect_to_db
from etl.extract import extract_data
from etl.load import load_data
import sqlite3

class RunPipeline():
    def __init__(self,db_name,master_file,incremental_column,group_by):
        self.db_name = db_name
        self.master_file = master_file
        self.incremental_column=incremental_column
        self.group_by=group_by
        
    def get_connection(self) :
        try:
            connect = connect_to_db(self.db_name)
            return connect
        except sqlite3.Error :
            "Database Connection Unsuccessful"
        
    def run(self) :
            conn = self.get_connection()
            try :
                data = extract_data(self.master_file)
            except :
                print("Source File could not be parsed")
            for files,schema in data :
                try :
                    load_data(files,conn,schema,self.incremental_column,self.group_by)
                except :
                    print("Some error occured while loading data into target table")
            conn.close()

        
   
        
if __name__ == '__main__':
    pipe = RunPipeline('hospital.db',r'data/master_run.csv','Last_Consulted_Date','Country')
    pipe.run()
    

