from DbConnector import DbConnector
from tabulate import tabulate

if __name__ == "__main__":

        # establish database connection
        connection = DbConnector()
        db_connection = connection.db_connection
        cursor = connection.cursor

        # create and define table Users
        query = """CREATE TABLE IF NOT EXISTS User (
            id VARCHAR(100) NOT NULL PRIMARY KEY,
            has_labels BOOLEAN)
        """
        cursor.execute(query)
        db_connection.commit()

        # create and define the table Activity
        query = """CREATE TABLE IF NOT EXISTS Activity (
            id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
            user_id VARCHAR(100),
            transportation_mode STRING,
            start_date_time DATETIME,
            end_datetime DATETIME,
            FOREIGN KEY (user_id) REFERENCES User(id)
            )
        """
        cursor.execute(query)
        db_connection.commit()

        # create and define the table TrackPoint
        query = """CREATE TABLE IF NOT EXISTS TrackPoint (
            id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
            activity_id INT,
            lat DOUBLE,
            lon DOUBLE,
            altitude INT,
            date_days DOUBLE,
            date_time DATETIME,
            FOREIGN KEY (activity_id) REFERENCES Activity(id)
            )
        """
        cursor.execute(query)
        db_connection.commit()