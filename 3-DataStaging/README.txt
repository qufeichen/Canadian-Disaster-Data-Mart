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
