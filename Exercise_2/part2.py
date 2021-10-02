from DbConnector import DbConnector
from haversine import haversine

class Queries: 
  def __init__(self):
    self.connection = DbConnector()
    self.db_connection = self.connection.db_connection
    self.cursor = self.connection.cursor
  
  def q1(self):
    query_user = """
      SELECT COUNT(*); 
      FROM User;
    """
    query_trackpoints = """
      SELECT COUNT(*); 
      FROM TrackPoint;
    """
    query_activites = """
      SELECT COUNT(*); 
      FROM Activites;
    """

    self.cursor.execute(query_user)
    res_user = self.cursor.fetchall()
    self.cursor.execute(query_trackpoints)
    res_trackpoints = self.cursor.fetchall()
    self.cursor.execute(query_activites)
    res_activities= self.cursor.fetchall()

    print('Query 1')
    # TODO: Print query answer
    self.new_task_line()
  
  def q2(self):
    # TODO: Send mail to stud.ass WHAT DOES IT MEEAN? PER USER?? Check if this is the correct interpretation
    query = """
      SELECT MAX(user_id), MIN(user_id), AVG(user_id);
      FROM Activites;
    """ 

    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 2')
    # TODO: Print query answer
    self.new_task_line()
  
  def q3(self):
    query = """
      SELECT TOP 10 user_id;
      FROM Activities; 
      ORDER BY COUNT(user_id) DESC;
    """
    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 3')
    # TODO: Print query answer
    self.new_task_line()
  
  def q4(self): 

    query = """
      SELECT DATEDIFF(start_date_time, end_date_time) AS DateDiff;
      FROM Activities;
      WHERE DateDiff = 1; 
    """

    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 4')
    # TODO: Print query answer
    self.new_task_line()

  def q5(self):
    # Send mail to stud.ass, we didn't understand what this query actually means
    pass 

  def q6(self): 
    # TODO: Ask ELIASSS
    pass 

  def q7(self): 
    query = """
      SELECT DISTINCT(user_id);
      FROM Activitiy;
      WHERE transportation_mode NOT "taxi";
    """

    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 7')
    # TODO: Print query answer
    self.new_task_line()

  def q8(self):
    query = """
      SELECT DISTINCT(transportation_mode) AS tp_mode;
      FROM Activity;
      WHERE 
    """
    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 8')
    # TODO: Print query answer
    self.new_task_line()


  def new_task_line(self):
    print("\n" + "---------------------" + "\n")

  def main(self):
    self.q1()
    self.q2()
    self.q3()
    self.q4()
    self.q5()
    self.q6()
    self.q7()
    self.q8()


if __name__ == "__main__":
    try:
        q = Queries()
        q.main()
    except Exception as e:
        print(e)