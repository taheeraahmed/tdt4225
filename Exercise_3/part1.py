from DbConnector import DbConnector
import os
from bson import objectid
import pprint

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
  def drop_collections(self):
    # Iterating through all collections and dropping them 
    for collection_name in self.db.list_collection_names(): 
      self.db[collection_name].drop()
    
    print('The collections has been dropped')
    self.line()

    # Create users, activities and trackpoints
  def insert_collections(self):
    users = self.db.create_collection('users')  
    activities = self.db.create_collection('activities')  
    trackpoints = self.db.create_collection('trackpoints')  

    print('The collections has been created ') 
    print(self.db.list_collection_names())
    self.line()

  # Insert data into 'users' collection 
  def insert_user_docs(self, collection_name='users'):
    print('Adding all users and their activities with their corresponding trackpoints....')
    
    # Making a set of all users which has labels
    with open(self.dataset_path + '/../labeled_ids.txt') as file:
      for line in file:
        user_id = line.strip()
        self.users_with_labels.add(user_id) 

    count_users = 0
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
        # Open one trajectory file with trackpoints here
        trajectory_file = open("{}/{}/Trajectory/{}".format(self.dataset_path,user_id, trajectory_filename))
        
        # Count lines in trajectory file from the 6th line 
        # Fix: Hehe we can fix the shady quick fix of count we want to
        count_trackpoints = -6
        for trackpoints in trajectory_file.readlines():
          count_trackpoints+=1
        # Closing the file
        trajectory_file.close()
        # Checking if there are more than 2500 trackpoints in the trajectory file
        if count_trackpoints > 2500:
          pass
        # The user has less than 2500 trackpoints in a trajectory, so we can add the activity to the file
        else: 
          add_trajectory_filename.append(trajectory_filename)
        
      # Dictionary with an activitity a user has
      activities_user = dict()  
      # The user has labels, so the activity can be added directly from the labels.txt file AND we must also check for non-matching activities in 
      # the trajectory file in order to find out whether or not a user has some unlabeld activities as well
      if has_labels:
        labels_file = open("{}/{}/labels.txt".format(self.dataset_path, user_id))
        next(labels_file)
        
        # Adding some activities from labels.txt
        for line in labels_file.readlines():
          start_date_time, end_date_time, transportation_mode = line.strip().split("\t")
          activities_user["{} - {}".format(start_date_time.replace("/", "-"), end_date_time.replace("/", "-"))] = transportation_mode
        
        # Check if a user has not labeled some activities
        for trajectory_filename in add_trajectory_filename:  
          with open("{}/{}/Trajectory/{}".format(self.dataset_path, user_id, trajectory_filename)) as trajectory_file:
            lines = trajectory_file.readlines()
            start_date_time = lines[6].strip().split(",")[-2] + " " + lines[6].strip().split(",")[-1]
            end_date_time = lines[-1].strip().split(",")[-2] + " " + lines[-1].strip().split(",")[-1]
          if not "{} - {}".format(start_date_time, end_date_time) in activities_user:
            transportation_mode = None
            activities_user["{} - {}".format(start_date_time, end_date_time)] = transportation_mode
            #print('does this every happen?')

      # The user doesn't have any labels so the activity can NEVER be added directly from the labels.txt file
      else: 
        for trajectory_filename in add_trajectory_filename:
          with open("{}/{}/Trajectory/{}".format(self.dataset_path, user_id, trajectory_filename)) as trajectory_file:
            lines = trajectory_file.readlines()
            start_date_time = lines[6].strip().split(",")[-2] + " " + lines[6].strip().split(",")[-1]
            end_date_time = lines[-1].strip().split(",")[-2] + " " + lines[-1].strip().split(",")[-1]
          transportation_mode = None
          activities_user["{} - {}".format(start_date_time.replace("/", "-"), end_date_time.replace("/", "-"))] = transportation_mode
      

      # Checking if the user actually has any activities which has less than 2500 tp's
      if len(add_trajectory_filename) == 0:
        pass
      # Creating a list of documents given a user, and inserting this into the db
      else: 
        activity_docs = self.make_activity_doc(activities_user)
        self.insert_docs('activities', activity_docs)


      # TODO: Add users
      #   user_doc = {
      #     '_id': user_id,
      #     'has_labels': has_labels,
      #     'activity_ids': '[list of object references,....]'
      #   }
      #   user_docs.append(user_doc)
      # self.insert_docs(collection_name=collection_name, docs=user_docs)

      # Adding users to the database 
      self.make_user_doc(user_id,has_labels)


      count_users += 1
      print("Progression: {}%\n".format((count_users/len(self.user_list)*100)))
    print("\n All data has now been added to the db!! ")
    self.line()
  
  def make_user_doc(self,user_id,has_labels,activity_dict):
    pass

  def make_activity_doc(self,activity_dict):
    # TODO: fix format of start and end date time?
    # TODO: 
    # activity_doc = autoincrementing int??
    #   'start_date_time': ISODate?
    #   'end_date_time': ISODate?
    #   'transportation_mode': string
    # }
    activity_docs = list()
    for activity_key in activity_dict:
      start_date_time = activity_key.split(' - ')[0]
      end_date_time = activity_key.split(' - ')[1]
      activity_doc = {
        '_activity_id':  objectid.ObjectId(),
        'start_date_time': start_date_time,
        'end_date_time': end_date_time,
        'transportation_mode': activity_dict[activity_key], 
      }
      activity_docs.append(activity_doc)
    return activity_docs

    # TODO: Do we want activity to have many TP object references?
    # Or many TP objects referencing the same corresponding activity?
  


  # Insert data into the 'trackpoints' collection 
  def insert_docs(self,collection_name, docs):
    collection = self.db[collection_name]
    collection.insert_many(docs)

  def line(self):
    print('-----------------------------------------------\n')


def main():
  test = None
  try:
    db = CreateCollections()
    
    db.drop_collections()
    db.insert_collections()
    db.insert_user_docs()
    #db.insert_docs()
  except Exception as e:
      print("ERROR: Failed to use database:", e)
  finally:
      if test:
          test.connection.close_connection()


if __name__ == '__main__':
    main()