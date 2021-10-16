from DbConnector import DbConnector
import os

class CreateCollections: 
  def __init__(self):
    # Establish database connection 
    self.connection = DbConnector()
    self.client = self.connection.client
    self.db = self.connection.db
    self.dataset_path = 'dataset/Data'
  
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
    print('Adding users, activities and trackpoints....')
    user_list = os.listdir(self.dataset_path)
    

    user_has_labels = set()
    with open(self.dataset_path + '/../labeled_ids.txt') as file:
      for line in file:
        user_id = line.strip()
        user_has_labels.add(user_id) 
    user_docs = []

    # TODO: Add array of references to activity object in each user document 
    for user_id in user_list:
      has_labels = 1 if user_id in user_has_labels else 0
      self.insert_activity_docs(user_id=user_id, has_label=has_labels)
      user_doc = {
        '_id': user_id,
        'has_labels': has_labels,
        #'activity_ids': '[object references,....]'
      }
      user_docs.append(user_doc)
    
    self.insert_docs(collection_name=collection_name, docs=user_docs)

    print('{} users has been added'.format(len(user_docs)))
    self.line()

  # Insert data into the 'activities' collection
  # Do we want activity to have many TP object references?
  # Or many TP objects referencing the same corresponding activity?

  # TODO: We don't need this, but the comments are nice 
  def insert_activity_docs(self, user_id, has_label):
    # Do not add activitites which have more than 2500 TPs
    """
    [
      {
        id: int
        transportation_mode: string
        start_datetime: ISODate (same as datetime)
        end_date_time: ISODate
      },
      .....
    ]
    """
    # TODO: This is empty when we go out of the if-sentence?? 
    activity_docs = []


    if has_label: 
      labels_file = open("{}/{}/labels.txt".format(self.dataset_path,user_id))
      # Skipping first line in labels_file
      next(labels_file)
      for line in labels_file.readlines():
        start_date_time, end_date_time, transportation = line.strip().split('\t')
        # Find reference to the object itself in the database? And append it to activities
        # TODO: Reformat starte_date_time and end_date_time
        activity_doc = [{
          'start_date_time':start_date_time,
          'end_date_time': end_date_time,
          'transportation_mode': transportation
        }]
        self.insert_docs(collection_name='activities', docs=activity_doc)
        # activity_docs.append(activity_doc)
    # Adding the activities for the user into the db 
    # self.insert_docs(collection_name='activities', docs=activity_docs)

    # TODO: Add the users without labels
    # for activity_filename in os.listdir("{}/{}/Trajectory/".format(self.dataset_path,user_id)):
      

    # 1. Find all activites for user_id that have less than 2500 TPs
    # 2. Fetch all activities belonging to user_id
    # 3. Return this list of activities belonging to user_id

    print('Adding activities for user {}...'.format(user_id))
    #return activity_ids

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