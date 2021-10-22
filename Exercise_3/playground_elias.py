from haversine import haversine
from DbConnector import DbConnector
from pprint import pprint
import pyfiglet
from datetime import datetime, timedelta


class AnsweringQueries: 
  def __init__(self):
    # Establish database connection 
    self.connection = DbConnector()
    self.client = self.connection.client
    self.db = self.connection.db

  def query_4(self):

    # returns all Activities without filter but projects just three fields
    result = list(self.db.Activity.aggregate([
      {'$project': {'_user_id': 1, 'start_date_time': 1, 'end_date_time': 1}}
    ]))

    users_that_started_an_activity_in_one_day_and_ended_in_other_day = set()

    for r in result:
      increased_start_date_time = r['start_date_time'] + timedelta(days=1)
      end_date_time = r['end_date_time']

      # idea: take the start_date_time and "increment" it by one day
      # then check if it has the same day, month and year as end_date_time

      # date() returns string with year, month, day of month
      if increased_start_date_time.date() == end_date_time.date():
        users_that_started_an_activity_in_one_day_and_ended_in_other_day.add(r['_user_id'])

    print('number of users that started and activity in one day and ended it on another day: {}'.format(
      len(users_that_started_an_activity_in_one_day_and_ended_in_other_day)
    ))

def main():
  test = None
  try:
    queries = AnsweringQueries()
    # queries.query_1()
    # queries.query_2()
    # queries.query_3()
    queries.query_4() 
    # queries.query_5()
    # queries.query_6() 
    # queries.query_7() 
    # TODO: queries.query_8() 
    # TODO: queries.query_9()
    # queries.query_10()  
    #queries.query_12() 
    # TODO: queries.query_12() 
  except Exception as e:
      print("ERROR: Failed to use database:", e)
  finally:
      if test:
          test.connection.close_connection()


if __name__ == '__main__':
    main()