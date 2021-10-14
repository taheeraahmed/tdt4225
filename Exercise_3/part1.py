from DbConnector import DbConnector
import os

class PartOne: 
  def __init__(self):
    self.connection = DbConnector()
    self.client = self.connection.client
    self.db = self.connection.db
  
  # Create and define collections User, Activity, TP 
  def create_collections(self):
    pass

  # Insert data
  def insert_data(self):
    pass

def main():
  test = None
  try:
    test = PartOne()
    test.define_collections()
    test.insert_data()
  except Exception as e:
      print("ERROR: Failed to use database:", e)
  finally:
      if test:
          test.connection.close_connection()


if __name__ == '__main__':
    main()