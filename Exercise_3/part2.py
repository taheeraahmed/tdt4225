from DbConnector import DbConnector
from pprint import pprint
import pyfiglet

 

class AnsweringQueries: 
  def __init__(self):
    # Establish database connection 
    self.connection = DbConnector()
    self.client = self.connection.client
    self.db = self.connection.db
  """
  How many users, activities and trackpoints are there in the dataset (after it is 
  inserted into the database).
  """
  def query_1(self):
    # TODO: .count() is deprecated?? tried switching to .count_documents() 
    # But then I get 'Cursor' object has no attribute 'count_documents'
    # IT RETURNS THE CORRECT VALUE HOWEVER
    users = self.db.users.find().count()
    activities = self.db.activities.find().count()
    trackpoints = self.db.trackpoints.find().count()

    self.heading(1)
    print("#No. of users: {} ".format(users))
    print("#No. of activities: {} ".format(activities))
    print("#No. of trackpoints: {}".format(trackpoints))

    self.new_task_line()

  """
  Find the average, minimum and maximum number of activities per user.
  """
  def query_2(self):

    idk = self.db.users.aggregate([{'$sort: {user_activities: 1}'}])
  

    self.heading(2)

    print(idk)

    self.new_task_line()

  """
  Find the top 10 users with the highest number of activities.
  """
  def query_3(self):
    self.heading(3)

    self.new_task_line()

  """
  Find the number of users that have started the activity in one day and ended
  the activity the next day
  """
  def query_4(self):
    self.heading(4)

    self.new_task_line()

  """
  Find activities that are registered multiple times. You should find the query
  even if you get zero results
  """
  def query_5(self):
    self.heading(5)

    self.new_task_line()

  """
  An infected person has been at position (lat, lon) (39.97548, 116.33031) at
  time ‘2008-08-24 15:38:00’.  Find the user_id(s) which have been close to this
  person in time and space (pandemic  tracking). Close is defined as the same
  minute (60 seconds) and space (100 meters). (This is a simplification of the“unsolvable” problem given i exercise 2)
  """
  def query_6(self):
    self.heading(6)

    self.new_task_line()

  """
  Find all users that have never taken a taxi
  """
  def query_7(self):
    self.heading(7)

    self.new_task_line()

  """
  Find all types of transportation modes and count how many distinct users that
  have used the different transportation modes. Do not count the rows where the
  transportation mode is null
  """
  def query_8(self):
    self.heading(8)

  """
  a) Find the year and month with the most activities. 
  b) Which user had the most activities this year and month, and how many
  recorded hours do they have? Do they have more hours recorded than the user
  with the second most activities?
  """
  def query_9(self):
    self.heading(9)

  """
  Find the total distance (in km) walked in 2008, by user with id=112.
  """
  def query_10(self):
    self.heading(10)
  
  """
  Find the top 20 users who have gained the most altitude meters
  1. Output should be a table with (id, total meters gained per user).
  2. Remember that some altitude-values are invalid
  3. Tip: (tpn.altitude-tpn-1.altitude), tpn.altitude >tpn-1.altitude
  """
  def query_11(self):
    self.heading(11)
  """
  Find all users who have invalid activities, and the number of invalid activities peruser 
  1. An invalid activity is defined as an activity with consecutive trackpoints
    where the timestamps deviate with at least 5 minutes. 
  """
  def query_12(self):
    self.heading(12)
  
  """
  Printing a line for style points
  """
  def new_task_line(self):
    print('-----------------------------------------------\n')


  """
  Printing a header for style points
  :param number (int) - Task number
  """
  def heading(self,number):
    result = pyfiglet.figlet_format("Query {}".format(number), font = "digital" )
    print(result)

def main():
  test = None
  try:
    queries = AnsweringQueries()
    """ THIS TAKES LONG TIME, TAKE CARE IF YOU DECIED TO CHANGE STUFF"""
    queries.query_1()
    queries.query_2()
    #db.drop_collections()
    #db.insert_collections()
    #db.insert_user_activity_docs()
    #db.insert_docs()
  except Exception as e:
      print("ERROR: Failed to use database:", e)
  finally:
      if test:
          test.connection.close_connection()


if __name__ == '__main__':
    main()