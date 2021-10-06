from DbConnector import DbConnector
from haversine import haversine
from tabulate import tabulate

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
      SELECT user_id, count(*) AS count 
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
      SELECT GROUP_CONCAT(id), COUNT(*) AS count 
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
    # TODO: VERY LONG running time..
    query = """
      SELECT user_id, TrackPoint.lon, TrackPoint.lat, TrackPoint.date_time 
      FROM Activity 
      INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
    """
    # WHERE user_id LIKE '00_';

    self.cursor.execute(query)
    res = self.cursor.fetchall()
    
    users = dict()
    close_users = set()

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

        print("user_a={}, user_b={}, pairwise_trackpoint_comparisons: {}x{}".format(user_a_id, user_b_id, len(user_a_trackpoints), len(user_b_trackpoints)))

        for user_a_lon, user_a_lat, user_a_date_time in user_a_trackpoints:
          for user_b_lon, user_b_lat, user_b_date_time in user_b_trackpoints:
            distance = haversine((user_a_lat, user_a_lon), (user_b_lat, user_b_lon))
            if distance <= 0.1:
              seconds_delta = abs((user_a_date_time - user_b_date_time).total_seconds())
              if seconds_delta <= 60:
                close_users.add(user_a_id)
                close_users.add(user_b_id)
                print("\n\nUsers met!!!\n\n")


    print("Number of users that have been close to each other in time and space: {}".format(len(close_users)))



  def q7(self): 
    query = """
      SELECT DISTINCT id FROM User 
      WHERE id NOT IN (
        SELECT user_id 
        FROM Activity 
        WHERE transportation_mode = 'taxi'
      )
    """

    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 7: res: {}'.format(res))
    self.new_task_line()

  def q8(self):
    # TODO: 

    query = """
      SELECT COUNT(DISTINCT user_id), transportation_mode 
      FROM Activity 
      WHERE transportation_mode != '' 
      GROUP BY transportation_mode;
    """
    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 8: {}'.format(res))
    # TODO: Print query answer
    self.new_task_line()

  def q9(self):
    query = """
      SELECT COUNT(*) AS count, YEAR(start_date_time) AS year, MONTH(end_date_time) AS month 
      FROM Activity 
      GROUP BY year, month 
      ORDER BY count DESC 
      LIMIT 1;
    """

    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 9a: Year: {} Month: {} #Activities: {}'.format(res[0][1], res[0][2], res[0][0]))



    # TODO: hier müssen wir nochmal ran: Wir müssen ausrechnen, wie viel Zeit 
    # der User mit Aktivitäten verbringt, nicht wie viele Aktivitäten es insgesamt gibt

    query = """
      SELECT COUNT(*) as count, user_id 
      FROM Activity 
      WHERE YEAR(start_date_time) = '2008' AND MONTH(start_date_time) = '11' 
      GROUP BY user_id 
      ORDER BY count DESC 
      LIMIT 1
    """

    self.cursor.execute(query)
    res = self.cursor.fetchall()

    print('Query 9b: {}'.format(res))
    self.new_task_line()

  def q10(self):
    query = """
      SELECT Activity.id, TrackPoint.lat, TrackPoint.lon 
      FROM Activity 
      INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id 
      WHERE user_id = '112' AND transportation_mode ='walk' AND YEAR(TrackPoint.date_time) = '2008';
    """
    self.cursor.execute(query)
    res = self.cursor.fetchall()


    # group trackpoints by activity_id
    trackpoints_per_activity = dict()
    for activity_id, lat, lon in res:
      trackpoints_per_activity.setdefault(activity_id, list()).append((lat, lon))

    total_distance = 0

    for activity_id, coordinates in trackpoints_per_activity.items():
      activity_distance = 0

      # start with the first trackingpoint as current
      previous_coordinate = coordinates.pop(0)

      # calculate the distance for every previous to every current trackpoint and add this distance to the counter
      for current_coordinate in coordinates:
        total_distance += haversine(previous_coordinate, current_coordinate)

    print('Query 10: distance:{}'.format(total_distance))
    self.new_task_line()

  def q11(self):
    # TODO: 

    query = """
      SELECT Activity.user_id, Activity.id, TrackPoint.altitude 
      FROM Activity 
      INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
    """
    self.cursor.execute(query)
    res = self.cursor.fetchall()

    # sort every row into a nested dict for the case the query isn't ordered by (user_id, activity_id)
    altitudes_by_activity_id_by_user_id = dict()
    for user_id, activity_id, current_altitude in res:
      altitudes_by_activity_id_by_user_id.setdefault(user_id, dict()).setdefault(activity_id, list()).append(current_altitude)

    gained_altitude_per_user_id = list()

    # calculate gained altitude for every user
    for user_id, altitudes_by_activity in altitudes_by_activity_id_by_user_id.items():
      user_gained_altitude_sum = 0


      for activity_id, altitudes in altitudes_by_activity.items():
        activity_gained_altitude_sum = 0

        # use the first altitude as reference
        previous_altitude = altitudes.pop(0)

        # for every next altitude calculate diff to previous
        for current_altitude in altitudes:

          print("user_id={},activity_id={},altitude={}".format(user_id, activity_id, current_altitude))

          # check if altitude is valid..
          if current_altitude != -777 and current_altitude > previous_altitude:
            activity_gained_altitude_sum += (current_altitude - previous_altitude)
            
        user_gained_altitude_sum += activity_gained_altitude_sum

      gained_altitude_per_user_id.append((user_id, user_gained_altitude_sum))
      # print("user_id={}, gained_altitude={}".format(user_id, user_gained_altitude_sum))

    # sort list
    gained_altitude_per_user_id_ranks = sorted(gained_altitude_per_user_id, key=lambda tup: tup[1])[-20:].reverse()

    print('Query 11: \n')
    
    for rank, user_id, gained_altitude in enumerate(gained_altitude_per_user_id):
      print("Rank {}: user_id: {}, gained altitude: {}".format(rank + 1, user_id, gained_altitude))

    self.new_task_line()

  def q12(self):
    # TODO: 

    query = """
      SELECT Activity.user_id, Activity.id, TrackPoint.date_time 
      FROM Activity 
      INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
    """
    self.cursor.execute(query)
    res = self.cursor.fetchall()

    # sort every row into a nested dict for the case the query isn't ordered by (user_id, activity_id)
    date_times_by_activity_id_by_user_id = dict()
    for user_id, activity_id, current_date_time in res:
      date_times_by_activity_id_by_user_id.setdefault(user_id, dict()).setdefault(activity_id, list()).append(current_date_time)

    users_with_invalid_activities = dict()

    # calculate gained altitude for every user
    for user_id, date_times_by_activity in date_times_by_activity_id_by_user_id.items():

      invalid_activity_counter = 0

      for activity_id, date_times in date_times_by_activity.items():

        invalid_activity = False

        previous_date_time = date_times.pop(0)
        for current_date_time in date_times:

          # if abs time_difference is at least 5 mins
          diff = abs((current_date_time - previous_date_time).total_seconds())
          if diff >= 300:
            invalid_activity = True

            # activity invalid -> no need to check the other timestamps of this activity
            break
        
        if invalid_activity:
          invalid_activity_counter += 1

      users_with_invalid_activities[user_id] = invalid_activity_counter

    print('Query 12:')
    
    for user_id, n_invalid_activities in users_with_invalid_activities.iter():
      print('user_id: {}, number of invalid activities: {}'.format(user_id, n_invalid_activities))
    self.new_task_line()

  def new_task_line(self):
    print("\n" + "---------------------" + "\n")

  def main(self):
    # self.q1()
    # self.q2()
    # self.q3()
    # self.q4()
    # self.q5()
    # self.q6()
    # self.q7()
    # self.q8()
    # self.q9()
    # self.q10()
    # self.q11()
    self.q12()


if __name__ == "__main__":

  q = Queries()
  q.main()
