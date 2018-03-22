# script to extract and transform the data and load all rows into the data mart

import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os
import datetime
import re

# dependencies: install these before running!
# pip install pandas
# pip install xlrd
# pip install psycopg2
# pip install sqlalchemy

def main():

    # pandas postgres engine
    # engine = create_engine('postgresql://qufeichen:@localhost:5432/disaster')

    # read in values from csv
    df = pd.read_excel(os.path.abspath("CanadianDisasterDatabase.xlsx"), header=0)
    df = df.dropna(how="all") # remove null rows

    # TEST
    # print(df)
    print(list(df))

    # - data frames for each dimension:
    #   date_df
    #   location_df
    #   disaster_df
    #   summary_df
    #   costs_df
    #   fact_df
    # - other enriched data (optional?)
    #   weather_df =
    #   population_stats_df =

    # location
    location_df = df[['PLACE','PLACE','PLACE','PLACE']].copy() # generate df for location dimension
    location_df.columns = ['city', 'province', 'country', 'canada'] # name columns
    # transform 'place' into location attributes
    location_df = location_df.apply(get_location, axis=1) #
    # remove duplicates
    location_df = location_df.drop_duplicates(keep='first').dropna(how="all") # filter out null values
    # create surrogate keys
    location_df['id'] = range(0, len(location_df)) # generate surrogate keys

    # print(location_df)

    # date
    # all date rows (start date and end date)
    date_columns = ['day', 'month', 'year', 'weekend', 'season-canada', 'season-international' ]
    date_df = df[['EVENT START DATE','EVENT START DATE','EVENT START DATE','EVENT START DATE', 'EVENT START DATE', 'EVENT START DATE']].copy() # generate df for location dimension
    date_df.columns = date_columns
    end_date_df = df[['EVENT END DATE','EVENT END DATE','EVENT END DATE','EVENT END DATE', 'EVENT END DATE', 'EVENT END DATE']].copy()
    end_date_df.columns = date_columns
    date_df = date_df.append(end_date_df, ignore_index=True)
    # transform date into date attributes
    date_df = date_df.apply(get_date, axis=1)
    # remove duplicates
    date_df = date_df.drop_duplicates(keep='first').dropna(how="all") # filter out null values
    # create surrogate keys
    date_df['id'] = range(0, len(date_df)) # generate surrogate keys

    # print(date_df)

    # disaster
    # can take columns as is from raw data
    disaster_columns = ['disaster_type', 'disaster_subgroup', 'disaster_group', 'disaster_category', 'magnitude-canada', 'utility-people_affected' ]
    disaster_df = df[['EVENT TYPE','EVENT SUBGROUP','EVENT GROUP','EVENT CATEGORY', 'MAGNITUDE', 'UTILITY - PEOPLE AFFECTED']].copy() # generate df for location dimension
    disaster_df.columns = disaster_columns
    # remove duplicates
    disaster_df = disaster_df.drop_duplicates(keep='first').dropna(how="all") # filter out null values
    # create surrogate keys
    disaster_df['id'] = range(0, len(disaster_df)) # generate surrogate keys

    # print(disaster_df)

    # summary
    summary_columns = ['summary', 'keyword1', 'keyword2', 'keyword3']
    summary_df = df[['COMMENTS', 'COMMENTS', 'COMMENTS', 'COMMENTS']]
    summary_df.columns = summary_columns
    # get keywords from summary
    summary_df = summary_df.apply(get_keywords, axis=1)
    # create surrogate keys
    summary_df['id'] = range(0, len(summary_df)) # generate surrogate keys

    print(summary_df)


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

    # return None if not a datetime object
    if not isinstance(date_time, datetime.date):
        return [None, None, None, None, None, None]

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

    return [date_time.day, date_time.month, date_time.year, weekend, season_canada, season_international]

def get_keywords(summary):
    # TODO: remove stopwords
    # randomly choose three words

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
    print(keywords)

    if len(keywords) < 3:
        return [summary, "", "", ""]
    else:
        return [summary, keywords[0], keywords[1], keywords[2]]

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
