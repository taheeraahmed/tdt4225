from DbConnector import DbConnector
from haversine import haversine

class Queries: 
  def __init__(self):
    self.connection = DbConnector()
    self.db_connection = self.connection.db_connection
    self.cursor = self.connection.cursor
  
  def q1(self):
    query_user = """
      SELECT COUNT(*) 
      FROM User;
    """
    query_activity = """
      SELECT COUNT(*)  
      FROM Activity;
    """
    query_trackpoint = """
      SELECT COUNT(*)  
      FROM TrackPoint;
    """

    self.cursor.execute(query_user)
    res_user = self.cursor.fetchall()
    self.cursor.execute(query_activity)
    res_activity= self.cursor.fetchall()
    self.cursor.execute(query_trackpoint)
    res_trackpoint = self.cursor.fetchall()

    print('Query 1: query_user={}, query_trackpoint={}, query_activity={}'.format(res_user, res_activity, res_trackpoint))
    self.new_task_line()
  
  def q2(self):
    # TODO: Send mail to stud.ass WHAT DOES IT MEEAN? PER USER?? Check if this is the correct interpretation
    query = """
      SELECT MAX(user_id), MIN(user_id), AVG(user_id) 
      GROUP BY user_id 
      FROM Activity;
    """ 

    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 2')
    # TODO: Print query answer
    self.new_task_line()
  
  def q3(self):
    query = """
      SELECT user_id, count(*) as count 
      FROM Activity
      GROUP BY user_id 
      ORDER BY count DESC
      LIMIT 10;
    """
    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 3: res={}'.format(res))
    # TODO: Print query answer
    self.new_task_line()
  
  def q4(self): 
    query = """
      SELECT DISTINCT(user_id) 
      FROM Activity 
      WHERE DATEDIFF(end_date_time, start_date_time) = 1;
    """

    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 4: res={}'.format(res))
    self.new_task_line()

  def q5(self):
    query = """
      SELECT GROUP_CONCAT(id), COUNT(*) as count 
      FROM Activity 
      GROUP BY user_id, start_date_time, end_date_time 
      HAVING count > 1;
    """

    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 5: res={}'.format(res))
    # TODO: Print query answer
    self.new_task_line()

  def q6(self): 
    # TODO: in progress
    query = """
      SELECT user_id, TrackPoint.lon, TrackPoint.lat, TrackPoint.date_time 
      FROM Activity 
      INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
      WHERE user_id LIKE '00_';
    """

    self.cursor.execute(query)
    res = self.cursor.fetchall()
    
    users = dict()

    for line in res:
      user_id, lon, lat, date_time = line
    
      users.setdefault(user_id, list()).append((lon, lat, date_time))

    # make dict a list
    users = list(users.items())

    for user_index_a in range(1, len(users)):
      for user_index_b in range(user_index_a):
        # comparing each user to all users with lower index

        user_a_id, user_a_trackpoints = users[user_index_a]
        user_b_id, user_b_trackpoints = users[user_index_b]

        for user_a_lon, user_a_lat, user_a_date_time in user_a_trackpoints:
          for user_b_lon, user_b_lat, user_b_date_time in user_b_trackpoints:
            distance = haversine((user_a_lat, user_a_lon), (user_b_lat, user_b_lon))
            if distance <= 0.1:
              print(user_a_date_time)
              # print(user_a_id, user_b_id)




  def q7(self): 
    # TODO
    query = """
      SELECT DISTINCT(user_id);
      FROM Activity;
      WHERE transportation_mode NOT "taxi";
    """

    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 7')
    # TODO: Print query answer
    self.new_task_line()

  def q8(self):
    # TODO
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
    # self.q1()
    # self.q2()
    # self.q3()
    # self.q4()
    # self.q5()
    self.q6()
    # self.q7()
    # self.q8()


if __name__ == "__main__":

  q = Queries()
  q.main()
