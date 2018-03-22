# script to extract and transform the data and load all rows into the data mart

import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os
import datetime
import re
import numpy as np
# dependencies: install these before running!
# pip install pandas
# pip install xlrd
# pip install psycopg2
# pip install sqlalchemy

# change these config params to your db credentials
DATABASE_NAME = "CanadianDisasterDataMart"
DATABASE_USERNAME = "qufeichen"
DATABASE_PASSWORD = ""

# disabling SettingWithCopyWarning
# pd.options.mode.chained_assignment = None  # default='warn'

def main():

    # pandas postgres engine
    engine = create_engine('postgresql://qufeichen:@localhost:5432/CanadianDisasterDataMart')

    # read in values from csv
    df = pd.read_excel(os.path.abspath("CanadianDisasterDatabase.xlsx"), header=0)
    df = df.dropna(how="all") # remove null rows

    #   weather_df =
    #   population_stats_df =

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
    # location_df.to_sql("location", engine, index=False, if_exists='append')

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
    # date_df.to_sql("date", engine, index=False, if_exists='append')


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
    # disaster_df.to_sql("disaster", engine, index=False, if_exists='append')


    # summary
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
    # summary_df.to_sql("summary", engine, index=False, if_exists='append')


    # costs
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
    # costs_df.to_sql("costs", engine, index=False, if_exists='append')

    # ADDITIONAL DIMENSTIONS:
    # WEATHER
    weather_df = pd.DataFrame([[0, ""]], columns=['weather_key', 'description'])
    # weather_df.to_sql("weather_info", engine, index=False, if_exists='append')
    # POPULATION STATS
    population_stats_df = pd.DataFrame([[0, ""]], columns=['pop_stats_key', 'description'])
    # population_stats_df.to_sql("population_statistics", engine, index=False, if_exists='append')

    # facts
    fact_columns = ['start_date_key', 'end_date_key', 'location_key', 'disaster_key', 'summary_key', 'cost_key', 'pop_stats_key', 'weather_key', 'fatalities', 'injured', 'evacuated']
    fact_df = pd.DataFrame(index = df.index.values, columns=fact_columns)
    # print(fact_df)
    fact_df['start_date_key'] = df['EVENT START DATE'].apply(get_start_date_id)
    # print(fact_df)


def get_start_date_id(start_date):
    # TODO:
    return 1

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


# dictionary of province name mappings
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

# connect to db and execute commands
def execute_db_command(commands, fetch_value):

    try:
        conn = psycopg2.connect(dbname="ufo", user="qufeichen", password="")
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
