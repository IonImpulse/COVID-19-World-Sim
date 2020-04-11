import pandas as pd
import json
import numpy as np
import os
import csv
import multiprocessing
import math
'''
This is a python program to 
'''


class Country :
    '''
    Object to break up everything into countries
    Stores:
        cities (as objects),
        current order (free, shelter-in-place recommended, shelter-in-place mandatory),
        gdp-per-capita

    TODO: document methods

    '''

    def __init__(self, iso2, cities, current_order, gdp_captia) :
        self.iso2 = iso2        
        self.current_order = current_order
        self.gdp_captia = gdp_captia
        
        self.cities = self.create_cities(cities)
        


    def create_cities(self, cities) :
        cities_dict = {}

        #Iterate over the cities dataframe
        for index, row in cities.iterrows() :
            
            #Set up lat/lon as tuple
            temp_lat_lon = (float(row["Latitude"]), float(row["Longitude"]))

            #Create city object and place it in dict by name
            cities_dict[row["City"]] = City(row["Population"], 0, 0, 0, temp_lat_lon, self.current_order, self.gdp_captia)

        return cities_dict


class City :
    '''
    Smallest object, acting as the actual population centers

    Stores:
        population,
        infected,
        immune,
        deceased,
        lat/lon,
    
    Inherits:
        current order (free, shelter-in-place recommended, shelter-in-place mandatory),
        gdp-per-capita
    '''

    def __init__(self, population, infected, recovered, deceased, lat_lon, current_order, gdp_per_captia) :
        self.population = population
        self.infected = infected
        self.recovered = recovered
        self.deceased = deceased
        self.lat_lon = lat_lon
        self.current_order = current_order
        self.gdp_per_capita = gdp_per_captia



def create_countries_threaded(name_lookup, cities_database, gdp_captia, iso2_iso3, output_dict) : 
    temp_countries_dict = {}
    
    #Iterate over the ISO2 to name lookup for convenience
    for iso2 in name_lookup :
        print("Setting up... ", name_lookup[iso2])
        #Get dataframe of cities by only selecting the rows with the proper ISO2 name
        temp_cities_df = cities_database.loc[cities_database["Country"] == iso2.lower()]

        #Check to make sure dataframe has cities and isn't empty 
        if len(temp_cities_df.index) > 0 :
                    
            try :
                #Get gdp by using ISO2 to ISO3 dict and looking up in the GDP JSON file, as GDP is using ISO3
                temp_gdp_captia = gdp_captia[iso2_iso3[iso2]]
                #Create country object and place it in a dict by name
                temp_countries_dict[name_lookup[iso2]] = Country(iso2, temp_cities_df, "None", temp_gdp_captia)
                    
            except Exception as e :
                print("Skipping", iso2)

    for item in temp_countries_dict :     
        output_dict[item] = temp_countries_dict[item] 


def iterate_day_threaded() :
    pass


class Virus_Simulator :
    '''
    Main class for this project
    Has many different function, with the main function being iterate_day()  
    '''
    
    def __init__(self) :
        '''
        Do nothing in __init__ as it may be necessary to generate data to load in other functions first
        Use method load_data() when ready to start
        '''
        pass

    def filter_raw_cities_datafile(self, path_to_file) :
        '''
        Method to grab the cities datafile at path_to_file and parse it into a smaller size
        Main data file simulator needs
        '''
        
        
        #Exclude city accent name as we don't need to load unnecessary data into memory
        used_columns = ["Country", "City", "Region", "Population", "Latitude", "Longitude"]
        
        try :
            #Read file into a pandas iter, with a relatively small chunksize so that it never takes up too much memory
            cities_iter = pd.read_csv(path_to_file, chunksize=10000, usecols=used_columns)

        except Exception as e :
            print(e)
            print("Raw data file missing or malformed.")
            print("Please download file from https://www.kaggle.com/max-mind/world-cities-database")
            os._exit(1)

        #Create empty dataframe with used_columns to append to
        cities_list = pd.DataFrame(columns=used_columns)

        #Loop over every chunk of 20,000
        for chunk in cities_iter :

            #Exclude rows (cities) that have a null population, as we can't use them for the simulation
            temp_list = chunk[chunk["Population"].notnull()]

            #Append the city to the empty dataframe
            cities_list = cities_list.append(temp_list, ignore_index=True)

        print(cities_list)

        #Create much smaller csv file with cities that have a non null population            
        cities_list.to_csv("Data\\cities_database.csv",index=False)

    def filter_raw_gdp_datafile(self, path_to_file) :
        #Create dict to dump to JSON
        gdp_data = {}
        
        #Don't use pandas, as it is only about 200 lines
        with open(path_to_file, 'r') as target :
            csv_reader = csv.reader(target)
            raw_gdp_data = []

            for row in csv_reader :
                #Detect when row is actually a data row and not a header
                if "GDP per capita (current US$)" in row :
                    raw_gdp_data.append(row)
    
        #Loop over raw data
        for row in raw_gdp_data :
            
            #Some regions don't have data
            if row[4] != "CD" :

                #Set newest_gdp to a placeholder         
                newest_gdp = None

                #Iterate over each data point, finding newest one
                for year in row[4:] :
                    if year.replace(" ", "") != "" :
                        newest_gdp = float(year)

                #Submit to dict as long as there is data
                if newest_gdp != None :
                    gdp_data[row[1]] = newest_gdp

        #Dump the dict to JSON file, to be read later as dict again.
        with open("Data\\gdp-capita.json", 'w') as target:
            json.dump(gdp_data, target)

    def load_data(self) :
        '''
        Loads cities database and JSON lookup files into memory
        Has try/except over everything, as loading files always can have problems
        Sets up everything for the iterate_day function
        '''
        
        try :
            self.cities_database = pd.read_csv("Data\\cities_database.csv")
        except Exception as e :
            print(e)
            print("Database file missing or malformed.")
            print("Please run the \"filter_raw_datafile\" to generate new database file.")
            print("Source: https://www.kaggle.com/max-mind/world-cities-database")
            os._exit(1)


        try:
            #ISO2 to country name
            with open("Data\\names.json") as target :
                self.name_lookup = json.load(target)

            #ISO2 to continent
            with open("Data\\continent.json") as target :
                self.continent_lookup = json.load(target)

            #ISO2 to ISO3
            with open("Data\\iso3.json") as target :
                self.iso2_iso3 = json.load(target)

            #Generated gdp lookup table
            with open("Data\\gdp-capita.json") as target :
                self.gdp_captia = json.load(target)

            #Generated age/key table
            with open("Data\\age-key.json") as target :
                self.age_key = json.load(target)

        except Exception as e :
            print(e)
            print("JSON lookup files missing or malformed.")
            print("Please place names.json, continent.json, and iso3.json in Data folder.")
            print("Source: http://country.io/data/")

    def create_countries(self) :
        '''
        Method to create countries from available data
        Will scale
        '''
        
        
        #Get num of usable threads
        #For an 8 core, 16 thread processor, num_of_thread will be 8
        self.num_of_threads = int(math.ceil(multiprocessing.cpu_count()/2))

        #Create lists of cities for each process to work on
        temp_cities_list = [{} for i in range(self.num_of_threads)]
        current_thread = 0

        for item in self.name_lookup :
            temp_cities_list[current_thread][item] = self.name_lookup[item]
            
            current_thread += 1

            if current_thread > 7 :
                current_thread = 0

        print(temp_cities_list)


        #Create dict to be filled with country objects
        manager = multiprocessing.Manager()
        self.countries_dict = manager.dict()
        
        #Create list of processes
        temp_process_list = []
        
        for i in range(self.num_of_threads) :

            #Create process with create_countries_threaded as target
            p = multiprocessing.Process(target=create_countries_threaded, args=(temp_cities_list[i], self.cities_database, self.gdp_captia, self.iso2_iso3, self.countries_dict))
            
            temp_process_list.append(p)

            p.start()

        for process in temp_process_list :
            process.join()


        #Setup dataframe for storing current state of everything, for easier access then looking up all values
        #This allows us to pass it as a value in the iterate_day_threaded function
        self.current_global_state = pd.Dataframe()




    def iterate_day(self) :
        manager = multiprocessing.Manager()
        self.countries_dict = manager.dict()
        
        #Create list of processes
        temp_process_list = []
        
        for i in range(self.num_of_threads) :
            print(i)
            p = multiprocessing.Process(target=create_countries_threaded, args=(temp_cities_list[i], self.cities_database, self.gdp_captia, self.iso2_iso3, self.countries_dict))
            
            temp_process_list.append(p)

            p.start()

        for process in temp_process_list :
            process.join()




if __name__ == '__main__':
    #Create simulator object
    sim = Virus_Simulator()

    '''
    Uncomment to create datafiles if missing
    Remember to download both cities list and gdp list from sources if doing so, and rename gdp file
    '''
    #sim.filter_raw_cities_datafile("Data\\worldcitiespop.csv)
    #sim.filter_raw_gdp_datafile("Data\\raw_gdp_capita.csv")

    
    #Load all data
    sim.load_data()
    
    #Create countries and cities, and setup the world for the simulation
    sim.create_countries()
