
from typing import DefaultDict
from DbConnector import DbConnector
from tabulate import tabulate
import os
import traceback
from datetime import datetime

if __name__ == "__main__":

        relative_dataset_path = "../dataset/Data"

        # create a set and insert every user id that has labels
        user_ids_with_label = set()
        with open(relative_dataset_path + "/../labeled_ids.txt") as file:
            for line in file:
                user_id = line.strip()
                user_ids_with_label.add(user_id)


        # establish database connection
        connection = DbConnector()
        client = connection.client
        db = connection.db

        # Iterating through all collections and dropping them 
        for collection_name in db.list_collection_names(): 
            db[collection_name].drop()

        print('The collections have been dropped')

        user = db.create_collection('User')  
        activity = db.create_collection('Activity')  
        trackpoint = db.create_collection('TrackPoint')  

        print('The collections have been created') 


        # LOOP: iterate over all users
        for user_id in os.listdir(relative_dataset_path):

            # True if user has labels
            user_has_labels = user_id in user_ids_with_label

            # insert user document
            db['User'].insert_one({
                '_id': user_id,
                'has_labels': user_has_labels
            })


            # for each user (listed in labeled_ids.txt) we try to read the labels.txt file and (if existing) create a dict with start_date_time and end_date_time as key and the transportation mode as value
            # if the file does not exist, the dict stays empty (same as always mismatching date_times)
            # create a dict for the transportation mode
            transportation_mode_dict = dict()
            if user_has_labels:
                labels_file = open("{}/{}/labels.txt".format(relative_dataset_path, user_id))

                # skip the first line
                next(labels_file)

                for line in labels_file.readlines():
                    start_date_time, end_date_time, transportation = line.strip().split("\t")
                    transportation_mode_dict["{} - {}".format(start_date_time.replace("/", "-"), end_date_time.replace("/", "-"))] = transportation

            # LOOP: iterate over every user's activity file
            for activity_filename in os.listdir("{}/{}/Trajectory/".format(relative_dataset_path, user_id)):
                print("User={},Activity={} ".format(user_id, activity_filename), end='')

                # open the activity file once, just to extract first and last date
                with open("{}/{}/Trajectory/{}".format(relative_dataset_path, user_id, activity_filename)) as activity_file:
                    lines = activity_file.readlines()
                    start_date_time = lines[6].strip().split(",")[-2] + " " + lines[6].strip().split(",")[-1]
                    end_date_time = lines[-1].strip().split(",")[-2] + " " + lines[-1].strip().split(",")[-1]

                transportation_mode = None
                if user_id in user_ids_with_label:
                    # now use the extracted dates to search in transportation_mode labels
                    if "{} - {}".format(start_date_time, end_date_time) in transportation_mode_dict:
                        transportation_mode = transportation_mode_dict["{} - {}".format(start_date_time, end_date_time)]
                        print("matched transportation_mode: key={},value={}".format("{} - {}".format(start_date_time, end_date_time), transportation_mode))
                



                with open("{}/{}/Trajectory/{}".format(relative_dataset_path, user_id, activity_filename)) as activity_file:
                    
                    # skip the first 7 lines
                    for _ in range(7):
                        next(activity_file)

                    # datapoints list for executemany()
                    trackpoint_docs = []

                    # LOOP: iterate over all track points
                    for i, line in enumerate(activity_file):
                        latitude, longitude, _, altitude, date_days, date_str, time_str = line.strip().split(",")
                        
                        # cast and building it back to a tuple for executemany()
                        trackpoint_docs.append({
                            'user_id': user_id,
                            'lat': float(latitude),
                            'lon': float(longitude),
                            'position': [float(longitude), float(latitude)],
                            'altitude': int(float(altitude)),
                            'date_days': float(date_days),
                            'date_time': datetime.strptime("{} {}".format(date_str, time_str), '%Y-%m-%d %H:%M:%S'),
                        })

                    if len(trackpoint_docs) > 2500:
                        print("has {} TrackPoints: skipping!".format(len(trackpoint_docs)))
                        continue

                    result = db['Activity'].insert_one({
                        '_user_id': user_id,
                        'transportation_mode': transportation_mode,
                        'start_date_time': datetime.strptime(start_date_time, '%Y-%m-%d %H:%M:%S'),
                        'end_date_time': datetime.strptime(end_date_time, '%Y-%m-%d %H:%M:%S')
                    })

                    # since mongodb creates object IDs itself we have to ask it for the just generated ID
                    activity_id = result.inserted_id

                    for trackpoint_doc in trackpoint_docs:
                        trackpoint_doc['_activity_id'] = activity_id


                    # else we continue with insertion into database
                    # create activity in database

                    db['TrackPoint'].insert_many(trackpoint_docs)
                    print("done")
                    
