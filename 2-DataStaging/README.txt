data_stage.py is the script that takes the given excel file, preprocesses the data, and inserts the data into the data mart.

--------------------------------------------------------------
Instructions for running data_stage.py
--------------------------------------------------------------

1. Create PostgreSQL database (suggested name = 'CanadianDisasterDataMart')

2. Update the config params in data_stage.py with the correct database credentials
  - DATABASE_NAME
  - DATABASE_USERNAME
  - DATABASE_PASSWORD

2. make sure all the following dependencies are installed:
  - pip install pandas
  - pip install xlrd
  - pip install psycopg2
  - pip install sqlalchemy
  - pip install numpy

3. run 'python data_stage.py'


--------------------------------------------------------------
The data folder contains a backup of the completed database, and exports(csv) of all of the completed tables(along with the queries used to export the tables)

The high level schematic can be found in high_level_schematic.png
A description of the steps taken to stage the data can be found in high_level_schematic.txt
