from DbConnector import DbConnector
from pprint import pprint
import pyfiglet
from datetime import datetime
from haversine import haversine

 

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
    users = self.db.User.find().count()
    activities = self.db.Activity.find().count()
    trackpoints = self.db.TrackPoint.find().count()

    self.heading(1)
    print("# users: {} ".format(users))
    print("# activities: {} ".format(activities))
    print("# trackpoints: {}".format(trackpoints))

    self.new_task_line()

  """
  Find the average, minimum and maximum number of activities per user.
  """
  def query_2(self):

    # directly collects all "rows" into list
    result = list(self.db.Activity.aggregate([
      {'$group': {'_id': '$_user_id', 'doc_count': { '$sum': 1 } } }
    ]))
    # result may look like this: [..., {'_id': '157', 'doc_count': 13}, ...]

  
    # sort list by 'doc_count' field
    result.sort(key=lambda d: d['doc_count'])

    avg_doc_count = sum(map(lambda d: d['doc_count'], result)) / len(result)
    
    self.heading(2) 

    print('minimum activity count: {} by user_id: {}, highest activity count: {} by user_id: {}, avg activity count: {}'.format(
      result[0]['doc_count'], result[0]['_id'],
      result[-1]['doc_count'], result[-1]['_id'],
      avg_doc_count
    ))


    self.new_task_line()

  """
  Find the top 10 users with the highest number of activities.
  """
  def query_3(self):

    # directly collects all "rows" into list
    result = list(self.db.Activity.aggregate([
      {'$group': {'_id': '$_user_id', 'doc_count': { '$sum': 1 } } }
    ]))

    # sort list by 'doc_count' field
    result.sort(key=lambda d: d['doc_count'])


    self.heading(3)

    for i in range(10):
      print('[Rank {}] user_id: {}, activity count: {}'.format(i+1, result[-(i+1)]['_id'], result[-(i+1)]['doc_count']))

    self.new_task_line()

  """
  Find the number of users that have started the activity in one day and ended
  the activity the next day
  """
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

    self.heading(4)

    self.new_task_line()

  """
  Find activities that are registered multiple times. You should find the query
  even if you get zero results
  """
  def query_5(self):
    """
    FINISHED
    """
    result = list(self.db.Activity.aggregate({
      '$group': { 
        '_id': { 'a': '$_user_id', 'b': '$start_date_time', 'c': '$end_date_time' },

        'count': { '$sum':  1 },

        'docs': { '$push': "$_id" }
      }},
      {
        '$match': {
          'count': { '$gt' : 1 }
        }
      }
    ))

    self.heading(5)

    print('equal Activities: ', result)

    self.new_task_line()

  """
  An infected person has been at position (lat, lon) (39.97548, 116.33031) at
  time ‘2008-08-24 15:38:00’.  Find the user_id(s) which have been close to this
  person in time and space (pandemic  tracking). Close is defined as the same
  minute (60 seconds) and space (100 meters). (This is a simplification of the“unsolvable” problem given i exercise 2)
  """
  def query_6(self): 
    # Get all TPs close to (39.97548, 116.33031) at time ‘2008-08-24 15:38:00’
    # +/- 60 sec
    # 100 metres 

    # TODO: we have to explain, that we added the coordinates as array and created a 2dsphere index for TrackPoint
    # this way its way faster to check for close coordinates
    # We also have to state, that we saved the user_id in the TrackPoint table
    

    result = list(self.db.TrackPoint.aggregate([
      {
        '$geoNear': {
          'near': { 'type': 'Point', 'coordinates': [ 116.33031, 39.97548 ] },
          'distanceField': 'dist.calculated',
          'maxDistance': 100,
          'includeLocs': 'dist.location',
          'spherical': True
        },
      },
      {
        '$match': {
          'date_time': {'$gte': datetime.strptime('2008-08-24 15:37:00', '%Y-%m-%d %H:%M:%S'), '$lte': datetime.strptime('2008-08-24 15:39:00', '%Y-%m-%d %H:%M:%S')}
        }
      }
    ]))

    self.heading(6)
    print("TrackPoints that have been close to [ 116.33031, 39.97548 ] around 2008-08-24 15:37:00")
    for r in result:
      print('user_id: {}, position: {}, date_time: {}'.format(r['_user_id'], r['position'], r['date_time']))
    self.new_task_line()

  """
  Find all users that have never taken a taxi
  FOR THE REPORT: Did this the other way around, found the users who have taken a taxi once and then just removed them from the list of all users
  """
  def query_7(self):
    # Taking all users who have taken a taxi once
    # 1. Find activities where the transportation_mode is taxi and get the user_ids
    result_taxi_once = list(self.db.Activity.aggregate([
      {'$match':{'transportation_mode':{'$eq':'taxi'}}},
      {'$unwind':'$_user_id'},
      {'$group':{'_id':'$_user_id'}}
    ]))

    taxi_once = [user_id['_id'] for user_id in result_taxi_once]

    # 2. Find all user id's, then just return the user id's which is not in the query??
    result_all_users = list(self.db.User.distinct("_id"))

    # 3. Do stuff with python
    never_taxi = []
    for all_user_id in result_all_users: 
      if not all_user_id in taxi_once:
        never_taxi.append(all_user_id)

    self.heading(7)
    print('These users have never taken a taxi:', never_taxi)
    self.new_task_line()

  """
  Find all types of transportation modes and count how many distinct users that
  have used the different transportation modes. Do not count the rows where the
  transportation mode is null
  """
  def query_8(self):
    self.heading(8)
    # Sorting all activities into different transportation modes and finding a list of all corresponding user_ids (they are not distinct)
    results = list(self.db.Activity.aggregate([
      { "$unwind": "$transportation_mode" },
      {
          "$group": {
              "_id": {"$toLower": '$transportation_mode'},
              "user_ids": { "$push": '$$ROOT._user_id' },
          }
      },
    ]))
    

    for result in results: 
      user_ids = set(result['user_ids'])
      
      unique_users = set()
      for user_id in user_ids: 
        unique_users.add(user_id)

      print("Transportation mode: {} #No. users: {}".format(result['_id'],len(unique_users)))

    self.new_task_line()

  """
  a) Find the year and month with the most activities. 
  b) Which user had the most activities this year and month, and how many
  recorded hours do they have? Do they have more hours recorded than the user
  with the second most activities?

  FOR THE REPORT: We assume that start_date_time decieds which month (if edge case)
  """
  def query_9(self):
    """Marianne"""
    self.heading(9)

    # a) Find year and month with most activities
    result = list(self.db.Activity.aggregate([
      {
        '$group': {
          '_id': {
            'year': { '$year': '$start_date_time' },
            'month': { '$month': '$start_date_time' },
          },
          'activity_count': { '$sum': 1 },
          },
        },
        { '$sort': { 'activity_count': -1 },
      },
    ]))


    print("a)\nYear: {} Month: {} Count: {}\n".format(result[0]['_id']['year'],result[0]['_id']['month'], result[0]['activity_count']))


    # b) Which user had the most activities this year and month, and how many
    # recorded hours do they have? Do they have more hours recorded than the user
    # with the second most activities?

    result = list(
      self.db.Activity.aggregate([
        {
          '$match': {
            'start_date_time': {
              '$lt': self.make_datetime_object('2008-12-01 00:00:00'),
              '$gte': self.make_datetime_object('2008-10-31 23:59:59'),
            },
          },
        },
        {
          '$group': {
            '_id': {
              'user_id': '$_user_id',
            },
            'count': { '$sum': 1 },
          },
        },
        { '$sort': { 'count': -1 } },
      ])
    )

    print("b)\n1. User ID: {}, count: {}".format(result[0]['_id']['user_id'], result[0]['count']))
    print("2. User ID: {}, count: {}".format(result[1]['_id']['user_id'], result[1]['count']))

    user_more = result[0]['count'] - result[1]['count']

    print('User {} has {} more activties than user {}'.format(result[0]['_id']['user_id'],user_more,result[1]['_id']['user_id']))
    self.new_task_line()

  

  """
  Find the total distance (in km) walked in 2008, by user with id=112.
  """
  def query_10(self):
    """TODO: explain how to create that index for activity_id"""

    # IMPORTANT TO MENTION: For performance-reasons we created an non-unique, non-sparse Index for '_activity_id' in TrackPoint
    # self.db.TrackPoint.create_index([
    #   {
    #     '_activity_id': 1
    #   },
    #   {
    #     'unique': False,
    #     'sparse': False,
    #   }
    # ])

    result = list(self.db.Activity.aggregate([
      {'$match': {'_user_id': '112', 'transportation_mode': 'walk'}},
      {'$lookup': {
        'from': 'TrackPoint',
        'localField': '_id',
        'foreignField': '_activity_id',
        'as': 'Activity_joined_TrackPoint'
      }}
    ]))

    total_walked_distance = 0

    # iterate over all the activities from user_id '112'
    for activity in result:
      table = list(activity.items())
      # structure of table:
      # [0]: ('_id', <value>)
      # [1]: ('_user_id', <value>)
      # [2]: ('transportation_mode', <value>)
      # [3]: ('start_date_time', <value>)
      # [4]: ('end_date_time', <value>)
      # [5]: ('Activity_joined_TrackPoint', [<all the Trackpoint records>])


      # use first TrackPoint as start reference
      previous_position = table[5][1].pop(0)['position']

      # iterate over all the joined TrackPoint entries
      for track_point in table[5][1]:
        distance = haversine(previous_position, track_point['position'])
        previous_position = track_point['position']

        # print("from {} to {}: {}km".format(previous_position, track_point['position'], distance))
        total_walked_distance += distance

    self.heading(10)
    print('total walked distance by user_id "112": {}'.format(total_walked_distance))
    self.new_task_line()
  

  
  """
  Find the top 20 users who have gained the most altitude meters
  1. Output should be a table with (id, total meters gained per user).
  2. Remember that some altitude-values are invalid
  3. Tip: (tpn.altitude-tpn-1.altitude), tpn.altitude >tpn-1.altitude
  """

  def query_11(self):
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

    self.heading(11)

    for rank, (user_id, gained_altitude) in enumerate(total_gained_altitude_by_user[:20]):
      print("Rank {}: user_id: {}, gained altitude: {}".format(rank + 1, user_id, gained_altitude))

    self.new_task_line()


  """
  Find all users who have invalid activities, and the number of invalid activities peruser 
  1. An invalid activity is defined as an activity with consecutive trackpoints
    where the timestamps deviate with at least 5 minutes. 
  """
  def query_12(self):
    result = list(self.db.Activity.aggregate([
      {'$lookup': {
        'from': 'TrackPoint',
        'localField': '_id',
        'foreignField': '_activity_id',
        'as': 'Activity_joined_TrackPoint'
      }},
      {'$project': {'_user_id': 1, 'Activity_joined_TrackPoint.date_time': 1}}, # only selects actual altitude values to be selected (saves huge amounts of bandwidth)
      # {'$limit': 100}
    ]))

    n_invalid_activities_by_user_id = dict()

    # iterate over all the activities from user_id '112'
    for activity in result:
      activity_is_invalid = False

      table = list(activity.items())
      # structure of table:
      # [0]: ('_id', <value>)
      # [1]: ('_user_id, <value>)
      # [2]: ('Activity_joined_TrackPoint.date_time', [<all the Trackpoint records>])


      # use first TrackPoint as start reference
      previous_date_time = table[2][1].pop(0)['date_time']

      # iterate over all the joined TrackPoint entries
      for track_point in table[2][1]:
        current_date_time = track_point['date_time']

        time_diff = (current_date_time - previous_date_time).total_seconds()
        previous_date_time = track_point['date_time']

        if time_diff >= 300.0:
          user_id = table[1][1]

          if user_id not in n_invalid_activities_by_user_id:
            n_invalid_activities_by_user_id[user_id] = 0

          n_invalid_activities_by_user_id[user_id] += 1

          # we found one invalid trackpoint -> don't need to check the other trackpoints
          break
        
    self.heading(12)
    
    print("Users with at least one invalid Activity:")
    for user_id, n_invalid_activities in n_invalid_activities_by_user_id.items():
      print("user_id: {}, n_invalid_activities: {}".format(user_id, n_invalid_activities))

    self.new_task_line()


  """
  Make a datetime object
  :param datetime_str (str) - '2009-01-03 01:21:34'
  """
  def make_datetime_object(self,datetime_str):
    datetime_object = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    return datetime_object
  
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
    # queries.query_1()
    # queries.query_2()
    # queries.query_3()
    # TODO: queries.query_4() 
    # queries.query_5()
    # queries.query_6() 
    # queries.query_7() 
    # queries.query_8() 
    # queries.query_9()
    # queries.query_10()  
    # queries.query_11() 
    # queries.query_12() 
  except Exception as e:
      print("ERROR: Failed to use database:", e)
  finally:
      if test:
          test.connection.close_connection()


if __name__ == '__main__':
    main()