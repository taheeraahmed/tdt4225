# Users 

Admin-user:

group11, group11, root

Name of db: strava

group11, group11, readwrite


# How to run 

## Activate the virtual environment by writing

`source venv/bin/activate` in the terminal when you are in the folder named Exercise_3

## Install the requirements 

`pip install -r requirments.txt` when you are in the folder named Exercise_3

## Connecting to the database

Now you need to set up the connection for the VM at ntnu. 
First you need to write in to the terminal:
`ssh username@tdt4225-xx.idi.ntnu.no`
Then you need to type in your FEIDE credentials. 

Now you can write `sudo mongod --bind_ip_all` in order to set up the server 

At last, open a new terminal window where you write `ssh username@tdt4225-xx.idi.ntnu.no` again with the same credentials. 

Now you can write `sudo mongo` and choose the db `use strava` in order to run the python script. 

## The db won't connect? 

Write this in the terminal `sudo lsof -iTCP -sTCP:LISTEN -n -P` which lists up all active TCP connections on the computer. If you see a proccess whose name is mongod, write `sudo kill #PID` and then you should be good to go! If not, I can't help you :((

## Now you should be good to go!! 

