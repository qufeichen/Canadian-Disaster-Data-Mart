# data staging script to extract and transform the data and load all rows into the data mart

import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os
import datetime
import re
import numpy as np

# dependencies: make sure these are installed before running!
# pip install pandas
# pip install xlrd
# pip install psycopg2
# pip install sqlalchemy
# pip install numpy

# change these config params to your db credentials
DATABASE_NAME = "CanadianDisasterDataMart"
DATABASE_USERNAME = "qufeichen"
DATABASE_PASSWORD = ""

# disabling SettingWithCopyWarning
# pd.options.mode.chained_assignment = None  # default='warn'

def main():

    # pandas postgres engine
    engine = create_engine('postgresql://' + DATABASE_USERNAME + ':' + DATABASE_PASSWORD + '@localhost:5432/' + DATABASE_NAME)

    # read in values from csv
    df = pd.read_excel(os.path.abspath("CanadianDisasterDatabase.xlsx"), header=0)
    df = df.dropna(how="all") # remove null rows
    # print(list(df))

    # LOCATION
    location_df = df[['PLACE','PLACE','PLACE','PLACE']].copy() # generate df for location dimension
    location_df.columns = ['city', 'province', 'country', 'canada'] # name columns
    # transform 'place' into location attributes
    location_df = location_df.apply(get_location, axis=1) #
    # remove duplicates
    location_df = location_df.drop_duplicates(keep='first')
    # create surrogate keys
    location_df['location_key'] = range(0, len(location_df)) # generate surrogate keys
    # print(location_df)

    # DATE
    # all date rows (start date and end date)
    date_columns = ['day', 'week', 'month','year', 'weekend', 'season_canada', 'season_international' ]
    date_df = df[['EVENT START DATE','EVENT START DATE','EVENT START DATE', 'EVENT START DATE','EVENT START DATE', 'EVENT START DATE', 'EVENT START DATE']].copy() # generate df for location dimension
    date_df.columns = date_columns
    end_date_df = df[['EVENT END DATE','EVENT START DATE', 'EVENT END DATE','EVENT END DATE','EVENT END DATE', 'EVENT END DATE', 'EVENT END DATE']].copy()
    end_date_df.columns = date_columns
    date_df = date_df.append(end_date_df, ignore_index=True)
    # transform date into date attributes
    date_df = date_df.apply(get_date, axis=1)
    # remove duplicates
    date_df = date_df.drop_duplicates(keep='first')
    # create surrogate keys
    date_df['date_key'] = range(0, len(date_df)) # generate surrogate keys
    # print(date_df)

    # DISASTER
    # can take columns as is from raw data
    disaster_columns = ['disaster_type', 'disaster_subgroup', 'disaster_group', 'disaster_category', 'magnitude', 'utility_people_affected' ]
    disaster_df = df[['EVENT TYPE','EVENT SUBGROUP','EVENT GROUP','EVENT CATEGORY', 'MAGNITUDE', 'UTILITY - PEOPLE AFFECTED']].copy() # generate df for location dimension
    disaster_df.columns = disaster_columns
    # remove duplicates
    disaster_df = disaster_df.drop_duplicates(keep='first')
    # create surrogate keys
    disaster_df['disaster_key'] = range(0, len(disaster_df)) # generate surrogate keys
    # print(disaster_df)

    # SUMMARY
    summary_columns = ['summary', 'keyword1', 'keyword2', 'keyword3']
    summary_df = df[['COMMENTS', 'COMMENTS', 'COMMENTS', 'COMMENTS']]
    summary_df.columns = summary_columns
    # get keywords from summary
    summary_df = summary_df.apply(get_keywords, axis=1)
    # change summary column data type from series to string
    summary_df.summary.apply('str')
    # create surrogate keys
    summary_df['summary_key'] = range(0, len(summary_df)) # generate surrogate keys
    # print(summary_df.to_string())

    # COSTS
    # note: column provincial_payments2 is a temp column
    costs_columns = ['estimated_total_cost', 'normalized_total_cost', 'federal_payments', 'provincial_payments', 'provincial_payments2', 'insurance_payments']
    costs_df = df[['ESTIMATED TOTAL COST', 'NORMALIZED TOTAL COST', 'FEDERAL DFAA PAYMENTS', 'PROVINCIAL DFAA PAYMENTS', 'PROVINCIAL DEPARTMENT PAYMENTS', 'INSURANCE PAYMENTS']]
    costs_df.columns = costs_columns
    # add together the two provincial payments (while taking null values into account)
    costs_df = costs_df.apply(get_provincial_payments, axis=1)
    # remove temp column provincial_payments2 and duplicates
    costs_df = costs_df.drop(['provincial_payments2'], axis=1)
    costs_df = costs_df.drop_duplicates(keep='first')
    # create surrogate keys
    costs_df['costs_key'] = range(0, len(costs_df)) # generate surrogate keys
    # print(costs_df.toString())

    # ADDITIONAL DIMENSTIONS:
    # WEATHER
    weather_df = pd.DataFrame([[0, ""]], columns=['weather_key', 'description'])
    # POPULATION STATS
    population_stats_df = pd.DataFrame([[0, ""]], columns=['pop_stats_key', 'description'])

    # # INSERT DIMENSIONS INTO DB
    location_df.to_sql("location", engine, index=False, if_exists='append')
    date_df.to_sql("date", engine, index=False, if_exists='append')
    disaster_df.to_sql("disaster", engine, index=False, if_exists='append')
    summary_df.to_sql("summary", engine, index=False, if_exists='append')
    costs_df.to_sql("costs", engine, index=False, if_exists='append')
    weather_df.to_sql("weather_info", engine, index=False, if_exists='append')
    population_stats_df.to_sql("population_statistics", engine, index=False, if_exists='append')

    # FACT TABLE
    df = df.apply(get_facts, axis=1)
    # rename columns
    fact_columns = ['start_date_key', 'end_date_key', 'location_key', 'disaster_key', 'summary_key', 'cost_key', 'pop_stats_key', 'weather_key', 'fatalities', 'injured', 'evacuated', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k']
    df.columns = fact_columns
    # drop unneeded columns
    df.drop(columns=['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'], inplace=True)
    # print(fact_df.toString())

    # insert facts into db
    df.to_sql("fact_table", engine, index=False, if_exists='append')

# method to format data in the location dimension
def get_location(location):

    # entire "PLACE" string is in location[0]
    place = location[0]

    # filter out entries consisting of only digits -> meaningless
    if not isinstance(place, str):
        return [None, None, None, None]

    # stop words to remove
    stop_words = ["in", "and", ""]

    # placeholders for resulting variables
    city = ""
    province = ""
    country = ""
    canada = ""

    # these locations do not follow the common pattern
    # check for these first
    exceptions = {
        "Quebec City" :["Quebec City", "QC"],
        "North Saskatchewan River" : ["North Saskatchewan River", "SK"]
    }

    # check for exceptions
    # if there are none, process as usual
    exception_found = False
    for e in exceptions.keys():
        if e in place:
            city = exceptions.get(e)[0]
            province = exceptions.get(e)[1]
            exception_found = True

    if not exception_found:
        # split into individual terms
        terms = place.split(" ")

        # get province and city
        for word in terms:
            result = get_province(word) # extract province name
            # if no province is listed, will return "N/A"
            if "N/A" not in result:
                province = result

                # remove province from city description
                city = place.replace(word, '')
                # check if there is a comma (indicates more than one location)
                # if yes, take the first location
                city = city.split(",")[0]

                # check if "and" occurs (indicates that location includes more than one place)
                # if yes, then take the first location (string before the 'and')
                if " and " in city:
                    city = city.split(" and ")[0]

                # remove stopwords
                for stop_word in stop_words:
                    city = city.replace(stop_word, '')

                # get rid of excess spaces
                city = re.sub(' +', ' ', city)
                city = city.strip()
                break

    # determine country and if in canada from the province
    if "N/A" in province:
        canada = False
        # TODO: how to find other countries?
        country = "Not Canada"
    else:
        canada = True
        country = "Canada"

    return [city, province, country, canada]

# helper method for location: dictionary of province name mappings
def get_province(argument):

    provinces = {
        "ON": "ON",
        "Ontario": "ON",
        "QC": "QC",
        "Quebec": "QC",
        "BC": "BC",
        "British Columbia": "BC",
        "AB": "AB",
        "Alberta": "AB",
        "ON": "ON",
        "NS": "NS",
        "Nova Scotia": "NS",
        "MB": "MB",
        "Manitoba": "MB",
        "SK": "SK",
        "Saskatchewan": "SK",
        "NB": "NB",
        "New Brunswick": "NB",
        "NL": "NL",
        "Newfoundland and Labrador": "NL",
        "PE": "PE",
        "PEI": "PE",
        "Prince Edward Island": "PE",
        "NT": "NT",
        "NT": "Northwest Territories",
        "YT": "YT",
        "Yukon": "YT",
        "NU": "NU",
        "Nunavut": "NU",
    }

    return provinces.get(argument, "N/A")

# method to format data in the date dimension
def get_date(date):

    # get date_time object
    date_time = date[0]

    # filter for objects taht are not datetime
    if not isinstance(date_time, datetime.datetime):
        return [None, None, None, None, None, None, None]

    # filer for null objects
    if date_time != date_time:
        return [None, None, None, None, None, None, None]

    # check if it is a weekend
    weekend = False

    if "Sunday" in date_time.weekday_name or "Saturday" in date_time.weekday_name:
        weekend = True

    # determine the season
    season_canada = ""
    season_international = ""
    if (date_time.month >= 3 and date_time.month <6):
        season_canada = "Spring"
        season_international = "Spring"
    elif (date_time.month >= 7 and date_time.month <9):
        season_canada = "Summer"
        season_international = "Summer"
    elif (date_time.month >= 9 and date_time.month <12):
        season_canada = "Autumn"
        season_international = "Autumn"
    else:
        season_canada = "Winter"
        season_international = "Winter"

    return [date_time.dayofweek+1, date_time.week, date_time.month, date_time.year, weekend, season_canada, season_international]

# method to extract keywords for the summary dimension
def get_keywords(summary):

    # words = summary.to_string().split()
    words = summary[0]

    # return None if not a string object
    if not isinstance(words, str):
        return [None, None, None, None]

    # Keywords: chose three words with longest length as keywords
    words = words.split()

    # build dictionary of word : character count
    my_dict = {}
    for word in words:
        my_dict[word] = len(word)

    # sort by length of word
    keywords = sorted(my_dict, key=my_dict.get, reverse=True)[:3]

    # if not enough words in summary for keywords, do not add
    if len(keywords) < 3:
        return [summary[0], "", "", ""]
    else:
        return [summary[0], keywords[0], keywords[1], keywords[2]]

# method to sum the provincial payments for the costs dimension
def get_provincial_payments(costs):
    # return sum of payment1 and payment2
    # values can be null, so must check
    estimated_total_cost = costs[0]
    normalized_total_cost = costs[1]
    federal_payments = costs[2]
    provincial_payments1 = costs[3]
    # payment 2 is a temp column, will not be used
    provincial_payments2 = costs[4]
    insurance_payments = costs[5]

    # set provincial_payments1 = provincial_payments1 + provincial_payments2, but check for null values
    if np.isnan(provincial_payments1):
        if np.isnan(provincial_payments2):
            provincial_payments1 = np.nan

        else:
            # return provincial_payments2
            provincial_payments1 = provincial_payments2
    else:
        if np.isnan(provincial_payments2):
            # return provincial_payments1
            provincial_payments1 = provincial_payments1

        else:
            # return provincial_payments1 + provincial_payments2
            provincial_payments1 = provincial_payments1 + provincial_payments2

    payment2 = np.nan
    return [estimated_total_cost, normalized_total_cost, federal_payments, provincial_payments1, provincial_payments2, insurance_payments]

# method to format data in fact table
def get_facts(line):

    # get params for start_date
    start_date = line['EVENT START DATE']
    start_date_array = []
    # filter for objects taht are not datetime
    if not isinstance(start_date, datetime.datetime):
        start_date_array = [None, None, None, None]
    # filer for null objects
    elif start_date != start_date:
        start_date_array = [None, None, None, None]
    else:
        start_date_array = [start_date.isoweekday(), start_date.strftime('%W'), start_date.month, start_date.year]

    # get params for end_date
    end_date = line['EVENT END DATE']
    end_date_array = []
    # filter for objects taht are not datetime
    if not isinstance(end_date, datetime.datetime):
        end_date_array = [None, None, None, None]
    # filer for null objects
    elif end_date != end_date:
        end_date_array = [None, None, None, None]
    else:
        end_date_array = [end_date.isoweekday(), end_date.strftime('%W'), end_date.month, end_date.year]

    # get location params
    location_array = get_location(list([line['PLACE'], line['PLACE']]))

    # get disaster params
    disaster_array = [line['EVENT TYPE'], line['EVENT SUBGROUP'], line['EVENT GROUP'], line['EVENT CATEGORY']]

    # get summary params
    summary_array = line['COMMENTS']

    # get costs params
    costs_array = [line['ESTIMATED TOTAL COST'], line['NORMALIZED TOTAL COST'], line['FEDERAL DFAA PAYMENTS'], line['INSURANCE PAYMENTS']]

    # retrieve keys
    start_date_key = get_date_id(start_date_array)
    end_date_key = get_date_id(end_date_array)
    location_key = get_location_id(location_array)
    disaster_key = get_disaster_id(disaster_array)
    summary_key = get_summary_id(summary_array)
    costs_key = get_cost_id(costs_array)
    # weather and pop_stats_key are fixed (no data yet)
    weather_key = 0
    population_stats_key = 0

    # measures -> grabbed directly from the line
    fatalities = line['FATALITIES']
    injured = line['INJURED / INFECTED']
    evacuated = line['EVACUATED']

    # return line
    return [start_date_key, end_date_key, location_key, disaster_key, summary_key, costs_key, weather_key, population_stats_key, fatalities, injured, evacuated, None,  None,  None,  None,  None,  None,  None,  None,  None,  None,  None]

# FACT TABLE HELPER METHODS (all method names ending with "_id")
# used to retrieve foreign keys from dimensional tables
def get_date_id(date):
    # date = [day, week, month, year]
    command = ("""SELECT date_key FROM date WHERE day={} AND week={} AND month={} AND year={}""".format(date[0], date[1], date[2], date[3]),)
    val = execute_db_command(command, True)
    if val is None:
        return None
    else:
        return val[0]

def get_location_id(place):
    # place = [city, province, country, canada]
    command = ("""SELECT location_key FROM location WHERE city='{}' AND province='{}' AND country='{}'""".format(place[0], place[1], place[2]),)
    val = execute_db_command(command, True)
    if val is None:
        return None
    else:
        return val[0]

def get_disaster_id(disaster):
    # disaster = [disaster_type, disaster_subgroup, disaster_group, disaster_category]
    command = ("""SELECT disaster_key FROM disaster WHERE disaster_type='{}' AND disaster_subgroup='{}' AND disaster_group='{}' AND disaster_category='{}'""".format(disaster[0], disaster[1], disaster[2], disaster[3]),)
    val = execute_db_command(command, True)
    if val is None:
        return None
    else:
        return val[0]

def get_summary_id(summary):
    # summary = 'COMMENTS'
    command = ("""SELECT summary_key FROM summary WHERE summary='{}'""".format(summary),)
    val = execute_db_command(command, True)
    if val is None:
        return None
    else:
        return val[0]

def get_cost_id(cost):
    # costs = [estimated_total_cost, normalized_total_cost, federal_payments, insurance_payments]
    command = ("""SELECT costs_key FROM costs WHERE estimated_total_cost={} AND normalized_total_cost={} AND federal_payments={} AND insurance_payments={}""".format(cost[0], cost[1], cost[2], cost[3]),)
    val = execute_db_command(command, True)
    if val is None:
        return None
    else:
        return val[0]

# connect to db and execute commands
def execute_db_command(commands, fetch_value):

    try:
        conn = psycopg2.connect(dbname=DATABASE_NAME, user=DATABASE_USERNAME, password=DATABASE_PASSWORD)
    except:
        print("I am unable to connect to the database.")

    cur = conn.cursor()
    try:
        for command in commands:
            cur.execute(command)

            if fetch_value:
                value = cur.fetchone()
                cur.close()
                return value

        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

if __name__ == "__main__":
    main()
