from haversine import haversine
from DbConnector import DbConnector
from pprint import pprint
import pyfiglet
from datetime import datetime


class AnsweringQueries: 
  def __init__(self):
    # Establish database connection 
    self.connection = DbConnector()
    self.client = self.connection.client
    self.db = self.connection.db

  def query_11(self):
    """Elias"""

    result = list(self.db.Activity.aggregate([
      {'$lookup': {
        'from': 'TrackPoint',
        'localField': '_id',
        'foreignField': '_activity_id',
        'as': 'Activity_joined_TrackPoint'
      }},
      {'$project': {'_user_id': 1, 'Activity_joined_TrackPoint.altitude': 1}} # only selects actual altitude values to be selected (saves huge amounts of bandwidth)
    ]))

    total_gained_altitude_by_user = dict()

    # iterate over all the activities from user_id '112'
    for activity in result:
      total_gained_altitude_by_current_activity = 0

      table = list(activity.items())
      # structure of table:
      # [0]: ('_id', <value>)
      # [1]: ('_user_id, <value>)
      # [2]: ('Activity_joined_TrackPoint.altitude', [<all the Trackpoint records>])


      # use first TrackPoint as start reference
      previous_altitude = table[2][1].pop(0)['altitude']

      # iterate over all the joined TrackPoint entries
      for track_point in table[2][1]:
        current_altitude = track_point['altitude']

        if current_altitude != -777 and current_altitude > previous_altitude:
          total_gained_altitude_by_current_activity += (current_altitude - previous_altitude)
        
        previous_altitude = track_point['altitude']

      # now add gained altitude of this activity to the user
      user_id = table[1][1]
      if user_id not in total_gained_altitude_by_user:
        total_gained_altitude_by_user[user_id] = 0

      total_gained_altitude_by_user[user_id] += total_gained_altitude_by_current_activity

    # collect dict as list and then sort it by second element (total_gained_altitude)
    total_gained_altitude_by_user = list(total_gained_altitude_by_user.items())
    total_gained_altitude_by_user.sort(key=lambda e: e[1])
    total_gained_altitude_by_user.reverse()

    for rank, (user_id, gained_altitude) in enumerate(total_gained_altitude_by_user[:20]):
      print("Rank {}: user_id: {}, gained altitude: {}".format(rank + 1, user_id, gained_altitude))


def main():
  test = None
  try:
    queries = AnsweringQueries()
    # queries.query_1()
    # queries.query_2()
    # queries.query_3()
    # TODO: queries.query_4() 
    # queries.query_5()
    # TODO: queries.query_6() 
    # queries.query_7() 
    # TODO: queries.query_8() 
    # TODO: queries.query_9()
    # queries.query_10()  
    queries.query_11() 
    # TODO: queries.query_12() 
  except Exception as e:
      print("ERROR: Failed to use database:", e)
  finally:
      if test:
          test.connection.close_connection()


if __name__ == '__main__':
    main()