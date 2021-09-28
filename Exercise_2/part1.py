
from typing import DefaultDict
from DbConnector import DbConnector
from tabulate import tabulate
import os




if __name__ == "__main__":

        main_path = "dataset/dataset/Data"

        # create a set and insert every user id that has labels
        user_has_labels = set()
        with open(main_path + "/../labeled_ids.txt") as file:
            for line in file:
                user_id = line.strip()
                user_has_labels.add(user_id)


        # establish database connection
        connection = DbConnector()
        db_connection = connection.db_connection
        cursor = connection.cursor

        # first step: delete all tables again
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


        # iterate over all users
        for user_id in os.listdir(main_path):

            # set to 1 if True, 0 else
            has_labels = 1 if user_id in user_has_labels else 0


            # create user in database
            query = "INSERT INTO User (id, has_labels) VALUES ({}, {});".format(user_id, has_labels)
            cursor.execute(query)
            db_connection.commit()

            # iterate over every user's activity
            for activity_filename in os.listdir("{}/{}/Trajectory/".format(main_path, user_id)):
                
                activity_id = activity_filename.split(".")[0]

                # TODO
                transportation_mode = "car"

                # open the activity file once, just to extract first and last date
                with open("{}/{}/Trajectory/{}".format(main_path, user_id, activity_filename)) as activity_file:
                    lines = activity_file.readlines()
                    start_date_time = lines[6].strip().split(",")[-2] + " " + lines[6].strip().split(",")[-1]
                    end_date_time = lines[-1].strip().split(",")[-2] + " " + lines[-1].strip().split(",")[-1]



                # insert activity into database
                query = """INSERT INTO 
                Activity (id, user_id, transportation_mode, start_date_time, end_date_time) 
                VALUES ({}, {}, {}, {}, {});""".format(int(activity_id), user_id, transportation_mode, start_date_time, end_date_time)

                print(query)

                cursor.execute(query)
                db_connection.commit()



                with open("{}/{}/Trajectory/{}".format(main_path, user_id, activity_filename)) as activity_file:
                    
                    # skip the first 7 lines
                    for _ in range(7):
                        next(activity_file)

                    # iterate over all track points
                    for i, line in enumerate(activity_file):
                        latitude, longitude, _, altitude, date_days, date_str, time_str = line.strip().split(",")

                        # we are just trying to create a unique id here :)
                        trackpoint_id = "{}{}{}".format(user_id, activity_filename, i)

                        # create activity in database
                        query = """INSERT INTO TrackPoint (id, activity_id, lat, lon, altitude, date_days, date_time) 
                        VALUES ({}, {}, {}, {}, {}, {}, {});""".format(trackpoint_id, activity_filename, float(latitude), float(longitude), int(altitude), float(date_days), date_str)

                        cursor.execute(query)
                        db_connection.commit()



                    exit()









                # print("User: {}, activity: {}".format(user_id, activity_id))