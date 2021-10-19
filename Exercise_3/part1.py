from DbConnector import DbConnector
import os
from bson import objectid
import pprint
from datetime import datetime

class CreateCollections: 
  def __init__(self):
    # Establish database connection 
    self.connection = DbConnector()
    self.client = self.connection.client
    self.db = self.connection.db
    # The relative path to the database
    self.dataset_path = 'dataset/Data'
    # A list with all users 
    self.user_list = os.listdir(self.dataset_path)
    # A set with all users with labels
    self.users_with_labels = set()
  
  """
  Dropping the collections
  """
  def drop_collections(self):
    # Iterating through all collections and dropping them 
    for collection_name in self.db.list_collection_names(): 
      self.db[collection_name].drop()
    
    print('The collections has been dropped')
    self.line()

    # Create users, activities and trackpoints
  
  """ 
  Creating the collections
  """
  def insert_collections(self):
    users = self.db.create_collection('users')  
    activities = self.db.create_collection('activities')  
    trackpoints = self.db.create_collection('trackpoints')  

    print('The collections has been created ') 
    print(self.db.list_collection_names())
    self.line()

  """ 
  Insert data into 'users' and 'activity' collection  
  """
  def insert_user_activity_docs(self):
    print('Adding all users and their activities with their corresponding trackpoints....')
    
    # Making a set of all users which has labels
    with open(self.dataset_path + '/../labeled_ids.txt') as file:
      for line in file:
        user_id = line.strip()
        self.users_with_labels.add(user_id) 

    # Used for checking the progression cause this takes tiiiime
    count_users = 0

    # Iterating through all the users
    for user_id in self.user_list:
      print('User {} is now being processed...'.format(user_id))
      # This list of all activities documents is going to be sent to the db
      activity_docs = []
      # Checking if the user has labels
      has_labels = 1 if user_id in self.users_with_labels else 0
      # A set of which trajectory filenames we can check because they have less than 2500 tps
      add_trajectory_filename = []
      # Finding the name of each trajectory file given a user
      for trajectory_filename in os.listdir("{}/{}/Trajectory".format(self.dataset_path, user_id)):
        trajectory_file = open("{}/{}/Trajectory/{}".format(self.dataset_path,user_id, trajectory_filename))
        # Fix: Hehe we can fix the shady quick fix of count we want to

        # Checking if there are more than 2500 trackpoints in the trajectory file
        # Count lines in trajectory file from the 6th line 
        count_trackpoints = -6
        for trackpoints in trajectory_file.readlines():
          count_trackpoints+=1
        trajectory_file.close()
        if count_trackpoints > 2500:
          pass
        else: 
          add_trajectory_filename.append(trajectory_filename)
        
      """
      Dictionary with all activitity a user has
      key: start-date-time - end-date-time (str) - format example: '2007-07-06 23:05:36 - 2007-07-07 12:40:40'
      value: [transportation_mode, objectID] (list) - objectID is the ID for the activity

      """
      activities_user = dict()  
      # The user has labels, so the activity can be added directly from the labels.txt file AND we must also check 
      # for non-matching activities in the trajectory file in order to find out whether or not a user has some
      # unlabeld activities as well
      if has_labels:
        labels_file = open("{}/{}/labels.txt".format(self.dataset_path, user_id))
        next(labels_file)
        
        # Adding some of the activities from labels.txt
        for line in labels_file.readlines():
          start_date_time, end_date_time, transportation_mode = line.strip().split("\t")
          activities_user["{} - {}".format(start_date_time.replace("/", "-"), end_date_time.replace("/", "-"))] = [transportation_mode, objectid.ObjectId()]
        
        # Check if a user has not labeled some activities which has less than 2500 TPs
        for trajectory_filename in add_trajectory_filename:  
          with open("{}/{}/Trajectory/{}".format(self.dataset_path, user_id, trajectory_filename)) as trajectory_file:
            lines = trajectory_file.readlines()
            start_date_time = lines[6].strip().split(",")[-2] + " " + lines[6].strip().split(",")[-1]
            end_date_time = lines[-1].strip().split(",")[-2] + " " + lines[-1].strip().split(",")[-1]
          if not "{} - {}".format(start_date_time, end_date_time) in activities_user:
            transportation_mode = None
            activities_user["{} - {}".format(start_date_time, end_date_time)] = [transportation_mode, objectid.ObjectId()]
            #print('does this every happen?')

      # The user doesn't have any labels so the activity can NEVER be added directly from the labels.txt file
      else: 
        for trajectory_filename in add_trajectory_filename:
          with open("{}/{}/Trajectory/{}".format(self.dataset_path, user_id, trajectory_filename)) as trajectory_file:
            lines = trajectory_file.readlines()
            start_date_time = lines[6].strip().split(",")[-2] + " " + lines[6].strip().split(",")[-1]
            end_date_time = lines[-1].strip().split(",")[-2] + " " + lines[-1].strip().split(",")[-1]
          transportation_mode = None
          activities_user["{} - {}".format(start_date_time.replace("/", "-"), end_date_time.replace("/", "-"))] = [transportation_mode, objectid.ObjectId()]
      

      # Checking if the user actually has any activities which has less than 2500 tp's
      if len(add_trajectory_filename) == 0:
        print('This user does not have any activities which has less than 2500 trackpoints')
        pass
      # Adding activities to the list which will be inserted into our db
      else:
        activity_docs = self.make_activity_doc(activities_user)
        self.insert_docs('activities', activity_docs)
        
        activity_keys = [key for key in activities_user]
        j = 0
        for trajectory_filename in add_trajectory_filename:
          with open("{}/{}/Trajectory/{}".format(self.dataset_path, user_id, trajectory_filename)) as trajectory_file:
            for _ in range(7):
              next(trajectory_file)
            # 2D array trackpoints given one activity (or trajectory file) 
            # [[tp1],[tp2],...]
            trackpoints = []
            for i, line in enumerate(trajectory_file):
              latitude, longitude, _, altitude, date_days, date_str, time_str = line.strip().split(",")
              activity_id = activities_user[activity_keys[j]][1]
              trackpoints.append([activity_id, float(latitude), float(longitude), int(float(altitude)), float(date_days), "{} {}".format(date_str, time_str)])
            j += 1
            #print(len(trackpoints), trajectory_filename)
            trackpoint_documents = self.make_trackpoint_doc(trackpoints)
            #print(trackpoints[0])
            self.insert_docs('trackpoints', trackpoint_documents)
      user_doc = self.make_user_doc(user_id,activity_docs)
      self.insert_docs('users', user_doc)
      
      # Just to check the progression
      count_users += 1
      progression = round(count_users/len(self.user_list)*100,1)
      print("Progression: {}%\n".format(progression))
    print("\n All data has now been added to the db!! ")
    self.line()

  """
  Makes a trackpoint document for each activity 
  :param trackpoints_lst (list) - A 2D array [[activity_id, lat, lon, alt, date_days, date_time],[tp2], [tp3], ...]
  :return trackpoint_docs (list) - A list of all the trackpoints and their one activity_id value
  """
  def make_trackpoint_doc(self,trackpoints_lst):
    trackpoint_docs=[]

    for trackpoint in trackpoints_lst: 
      #çççççprint(trackpoint)
      trackpoint = {
        'activity_id': trackpoint[0],
        'latitude': trackpoint[1],
        'longtitude': trackpoint[2],
        'altitude': trackpoint[3],
        'date_days': trackpoint[4],
        'date_time': self.make_datetime_object(trackpoint[5]),
      }
      trackpoint_docs.append(trackpoint)
    return trackpoint_docs

  """
  Making the user documents 
  :param user_id (int) 
  :param activity_docs (dict)
  :return user_doc (list) - a list of a "JSON" object of a user
  """
  def make_user_doc(self,user_id,activity_docs):
    activity_ids = [activity_doc['_activity_id'] for activity_doc in activity_docs ]
    user_doc = [{
      '_id': user_id,
      'has_labels': user_id in self.users_with_labels, 
      'user_activities': activity_ids
    }]
    return user_doc
  
  """
  Make a datetime object
  :param datetime_str (str) - '2009-01-03 01:21:34'
  """
  def make_datetime_object(self,datetime_str):
    datetime_object = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    return datetime_object

  """
  Making activity documents for one user
  :param activity_dict (dict) - a dictionary with all activities for one user
  :return activity_docs (list) - a list with "JSON" objects for all activities a user has done
  """
  def make_activity_doc(self,activity_dict):
    activity_docs = list()
    for activity_key in activity_dict:
      start_date_time= self.make_datetime_object(activity_key.split(' - ')[0])
      end_date_time = self.make_datetime_object(activity_key.split(' - ')[1])
      activity_doc = {
        '_activity_id':  activity_dict[activity_key][1],
        'start_date_time': start_date_time,
        'end_date_time': end_date_time,
        'transportation_mode': activity_dict[activity_key][0], 
      }
      activity_docs.append(activity_doc)
    return activity_docs

    # TODO: Do we want activity to have many TP object references?
    # Or many TP objects referencing the same corresponding activity?
    """
    Making the trackpoint documents 
    :param trackpoint_dict (dict) - should be a dictionary with all trackpoints given one activitiy
    :return trackpoint_docs (list) - should be list of dictionaries containg all trackpoints given one activity
    """
    def make_trackpoint_doc(self,trackpoint_dict):
      # tp doc={
      #   'id': id (int)
      #   'lat': double
      #   'long': double
      #   'altitude': int
      #   'date_days': double
      #   'date_time': datetime
      #   'activity_id': ObjectId
      # }
      pass
  
  """
  Inserting documents to the database
  :param collection_name (str) - name of the collection you want to insert data
  :param docs (list) - a list of "JSON" objects (hehe dictionaries) we want to insert
  """
  def insert_docs(self,collection_name, docs):
    collection = self.db[collection_name]
    collection.insert_many(docs)

  """
  Just printing a line
  """
  def line(self):
    print('-----------------------------------------------\n')


def main():
  test = None
  try:
    db = CreateCollections()
    """ THIS TAKES LONG TIME, TAKE CARE IF YOU DECIED TO CHANGE STUFF"""
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