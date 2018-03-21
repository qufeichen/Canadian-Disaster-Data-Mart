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
    print(df)

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

    # date -> start date and end date
    # iterate over df?

    # location
    location_df = df[['PLACE','PLACE','PLACE','PLACE']].copy() # generate df for location dimension
    location_df.columns = ['city', 'province', 'country', 'canada'] # name columns
    location_df = location_df.apply(get_location, axis=1) #
    location_df = location_df.drop_duplicates(keep='first').dropna(how="all") # filter out null values
    location_df['id'] = range(0, len(location_df)) # generate surrogate keys

    # print(location_df)
    # documents = documents.append({"ID": doc_id, "Passed Away": passed_away, "Discharge Summary": discharge_summary}, ignore_index=True)

def get_location(location):

    # entire "PLACE" string is in location[0]
    place = location[0]

    # filter out entries consisting of only digits -> meaningless
    if not isinstance(place, str):
        return [None, None, None, None]

    # split into individual terms
    terms = place.split(" ")

    # stop words to remove
    stop_words = ["in", "and", ""]

    # placeholders for resulting variables
    city = ""
    province = ""
    country = ""
    canada = ""

    # get province and city
    for word in terms:
        result = get_province(word) # extract province name
        # if no province is listed, will return "N/A"
        if "N/A" not in result:
            province = result

            # remove province from city description
            city = place.replace(word, '')

            # check if "and" occurs (indicates that location includes more than one place)
            # if yes, then take the first location (string before the 'and')
            if " and " in city:
                print("before" + city)
                city = city.split(" and ")[0]
                print("after" + city)
            # remove stopwords
            for stop_word in stop_words:
                city = city.replace(stop_word, '')
            # get rid of excess spaces
            city = city.replace(r'\s+', ' ')
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


# dictionary of province name mappings
def get_province(argument):
    provinces = {
        "ON": "ON",
        "Ontario": "ON",
        "QC": "QC",
        "Quebec City": "QC",
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
