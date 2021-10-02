
from typing import DefaultDict
from DbConnector import DbConnector
from tabulate import tabulate
import os
import mysql
import traceback




if __name__ == "__main__":

        relative_dataset_path = "dataset/dataset/Data"

        # create a set and insert every user id that has labels
        user_has_labels = set()
        with open(relative_dataset_path + "/../labeled_ids.txt") as file:
            for line in file:
                user_id = line.strip()
                user_has_labels.add(user_id)


        # establish database connection
        connection = DbConnector()
        db_connection = connection.db_connection
        cursor = connection.cursor

        # we drop all tables to achieve deterministic behaviour for every execution of our program
        query = "DROP TABLE TrackPoint;"
        cursor.execute(query)
        db_connection.commit()
        query = "DROP TABLE Activity;"
        cursor.execute(query)
        db_connection.commit()
        query = "DROP TABLE User;"
        cursor.execute(query)
        db_connection.commit()

        # create and define table Users
        query = """CREATE TABLE IF NOT EXISTS User (
            id VARCHAR(100) NOT NULL PRIMARY KEY,
            has_labels BOOLEAN NOT NULL)
        """
        cursor.execute(query)
        db_connection.commit()

        # create and define the table Activity
        query = """CREATE TABLE IF NOT EXISTS Activity (
            id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
            user_id VARCHAR(100) NOT NULL,
            transportation_mode VARCHAR(100) NOT NULL,
            start_date_time DATETIME NOT NULL,
            end_date_time DATETIME NOT NULL,
            FOREIGN KEY (user_id) REFERENCES User(id)
            )
        """
        cursor.execute(query)
        db_connection.commit()

        # create and define the table TrackPoint
        query = """CREATE TABLE IF NOT EXISTS TrackPoint (
            id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
            activity_id INT NOT NULL,
            lat DOUBLE NOT NULL,
            lon DOUBLE NOT NULL,
            altitude INT NOT NULL,
            date_days DOUBLE NOT NULL,
            date_time DATETIME NOT NULL,
            FOREIGN KEY (activity_id) REFERENCES Activity(id)
            )
        """
        cursor.execute(query)
        db_connection.commit()


        # LOOP: iterate over all users
        for user_id in os.listdir(relative_dataset_path):

            # set to 1 if True, 0 else
            has_labels = 1 if user_id in user_has_labels else 0

            # create user in database
            query = "INSERT INTO User (id, has_labels) VALUES ('{}', {});".format(user_id, has_labels)
            cursor.execute(query)
            db_connection.commit()



            # for each user we try to read the labels.txt file and (if existing) create a dict with start_date_time and end_date_time as key and the transportation mode as value
            # if the file does not exist, the dict stays empty (same as always mismatching date_times)
            # create a dict for the transportation mode
            transportation_mode_dict = dict()
            if user_id in user_has_labels:
                labels_file = open("{}/{}/labels.txt".format(relative_dataset_path, user_id))

                # skip the first line
                next(labels_file)

                for line in labels_file.readlines():
                    start_date_time, end_date_time, transportation = line.strip().split("\t")
                    transportation_mode_dict["{} - {}".format(start_date_time, end_date_time)] = transportation


            #print(transportation_mode_dict)


            # LOOP: iterate over every user's activity file
            for activity_filename in os.listdir("{}/{}/Trajectory/".format(relative_dataset_path, user_id)):
                print("User={},Activity={} ".format(user_id, activity_filename), end='')

                # open the activity file once, just to extract first and last date
                with open("{}/{}/Trajectory/{}".format(relative_dataset_path, user_id, activity_filename)) as activity_file:
                    lines = activity_file.readlines()
                    start_date_time = lines[6].strip().split(",")[-2] + " " + lines[6].strip().split(",")[-1]
                    end_date_time = lines[-1].strip().split(",")[-2] + " " + lines[-1].strip().split(",")[-1]

                transportation_mode = str()
                if user_id in user_has_labels:
                    # now use the extracted dates to search in transportation_mode labels
                    if "{} - {}".format(start_date_time, end_date_time) in transportation_mode_dict:
                        transportation_mode = transportation_mode_dict["{} - {}".format(start_date_time, end_date_time)]
                        print("matched transportation_mode: key={},value={}".format("{} - {}".format(start_date_time, end_date_time), transportation_mode))


                # insert activity into database (requires to reference user)
                query = """INSERT INTO 
                Activity (user_id, transportation_mode, start_date_time, end_date_time) 
                VALUES ({}, '{}', '{}', '{}');""".format(user_id, transportation_mode, start_date_time, end_date_time)

                try:
                    cursor.execute(query)
                    db_connection.commit()
                except Exception:
                    print("catched this..")
                    print(traceback.print_exc())


                # since mysql creates IDs itself (auto-increment) we have to query for this activity's ID
                activity_id = cursor.lastrowid



                with open("{}/{}/Trajectory/{}".format(relative_dataset_path, user_id, activity_filename)) as activity_file:
                    
                    # skip the first 7 lines
                    for _ in range(7):
                        next(activity_file)

                    # datapoints list for executemany()
                    datapoints = []

                    # LOOP: iterate over all track points
                    for i, line in enumerate(activity_file):
                        latitude, longitude, _, altitude, date_days, date_str, time_str = line.strip().split(",")
                        
                        # cast and building it back to a tuple for executemany()
                        datapoints.append((activity_id, float(latitude), float(longitude), int(float(altitude)), float(date_days), "{} {}".format(date_str, time_str)))

                    if len(datapoints) > 2506:
                        print("has {} TrackPoints: skipping!".format(len(datapoints)))
                        continue


                    # else we continue with insertion into database
                    # create activity in database
                    query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s);"

                    try:
                        # mass insert all datapoints (TrackPoint) at once
                        cursor.executemany(query, datapoints)
                        db_connection.commit()
                    except Exception:
                        print(traceback.print_exc())

                    print("done")
                    










                # print("User: {}, activity: {}".format(user_id, activity_id))