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

def haversine(lon_lat1, lon_lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(math.radians, list(lon_lat1) + list(lon_lat2))
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    # Radius of earth in kilometers is 6371
    km = 6371* c
    return km


class Country :
    '''
    Object to break up everything into countries
    Stores:
        cities (as objects),
        current order (free, shelter-in-place recommended, shelter-in-place mandatory),
        gdp-per-capita

    TODO: document methods

    '''

    def __init__(self, iso2, cities, current_order, gdp_captia, beds_per_1000) :
        self.iso2 = iso2        
        self.current_order = current_order
        self.gdp_captia = gdp_captia
        self.beds_per_1000 = beds_per_1000
        self.cities = self.create_cities(cities)
        
        self.ledger = pd.DataFrame(columns=list(cities.keys()))

    def create_cities(self, cities) :
        cities_dict = {}

        #Iterate over the cities dataframe
        for index, row in cities.iterrows() :
            
            #Set up lat/lon as tuple
            temp_lat_lon = (float(row["Latitude"]), float(row["Longitude"]))
            
            #Calculate number of hopsital beds per city, which changes with pop. and gdp
            hospital_beds_aval = row["Population"]/1000 * self.beds_per_1000
            
            #Create city object and place it in dict by name
            cities_dict[row["City"]] = City(row["Population"], 0, 0, 0, temp_lat_lon, self.current_order, self.gdp_captia, hospital_beds_aval)

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
        hospital beds available
    Inherits:
        current order (free, shelter-in-place recommended, shelter-in-place mandatory),
        gdp-per-capita
    '''

    def __init__(self, population, infected, recovered, deceased, lat_lon, current_order, gdp_per_captia, hospital_beds_aval) :
        self.population = population
        self.infected = infected
        self.recovered = recovered
        self.deceased = deceased
        self.lat_lon = lat_lon
        self.current_order = current_order
        self.gdp_per_capita = gdp_per_captia
        self.hospital_beds_aval = hospital_beds_aval



def create_countries_threaded(name_lookup, cities_database, gdp_captia, iso2_iso3, output_dict, beds_per_1000) : 
    temp_countries_dict = {}
    
    #Iterate over the ISO2 to name lookup for convenience
    for iso2 in name_lookup :
        print("Setting up... ", name_lookup[iso2])
        #Get dataframe of cities by only selecting the rows with the proper ISO2 name
        temp_cities_df = cities_database.loc[cities_database["Country"] == iso2.lower()]

        #Check to make sure dataframe has cities and isn't empty 
        if len(temp_cities_df.index) > 0 :
                    
            try :
                #Only lookup iso3 once
                iso3 = iso2_iso3[iso2]

                #Get gdp by using ISO2 to ISO3 dict and looking up in the GDP JSON file, as GDP is using ISO3
                temp_gdp_captia = gdp_captia[iso3]

                #Get beds per 100 by looking up iso3 code for JSON file
                temp_beds_per_1000 = beds_per_1000[iso3]

                #Create country object and place it in a dict by name
                temp_countries_dict[name_lookup[iso2]] = Country(iso2, temp_cities_df, "None", temp_gdp_captia, temp_beds_per_1000)
                    
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
    infection_rate = 0-1
    percent_social_distancing = 0-1
    government_action_timing = 0-1
    '''
    
    def __init__(self, infection_rate, percent_social_distancing, government_action_timing, time_in_hospital, hospitalization_rate, mortality_rate) :
        '''
        Sets paramaters to control simulation
        '''

        #How likely an encounter is to spread the virus
        self.infection_rate = infection_rate

        #How many people will social distance once order is given
        self.percent_social_distancing = percent_social_distancing

        #When governments will impose shelter in place order (percent of pop infected)
        self.government_action_timing = government_action_timing

        #How long people spend in hospital before dying or recovering
        self.time_in_hospital = time_in_hospital

        #What percentage of cases need hospitalization
        self.hospitalization_rate = hospitalization_rate

        #Rate of death. Doubled when not in hospitals.
        self.mortality_rate = mortality_rate

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

    def filter_raw_beds_datafile(self, path_to_file) :
        #Create dict to dump to JSON
        beds_data = {}
        
        #Don't use pandas, as it is only about 200 lines
        with open(path_to_file, 'r') as target :
            csv_reader = csv.reader(target)
            raw_bed_data = []

            for row in csv_reader :
                raw_bed_data.append(row)
        
        del raw_bed_data[0]

        for row in raw_bed_data :
            temp_list = [i for i in row[4:] if i.replace(" ", "") != ""]
            
            if len(temp_list) > 0 :
                beds_data[row[1]] = float(temp_list[-1])
        
        #Dump the dict to JSON file, to be read later as dict again.
        with open("Data\\bed-per-1000.json", 'w') as target:
            json.dump(beds_data, target)

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

            #Generated beds per 1000 table
            with open("Data\\beds-per-1000.json") as target :
                self.beds_per_1000 = json.load(target)

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

            if current_thread == self.num_of_threads :
                current_thread = 0

        #Create dict to be filled with country objects
        manager = multiprocessing.Manager()
        self.countries_dict = manager.dict()
        
        #Create list of processes
        temp_process_list = []
        
        for i in range(self.num_of_threads) :

            #Create process with create_countries_threaded as target
            p = multiprocessing.Process(target=create_countries_threaded, args=(temp_cities_list[i], self.cities_database, self.gdp_captia, self.iso2_iso3, self.countries_dict, self.beds_per_1000))
            
            temp_process_list.append(p)

            p.start()

        for process in temp_process_list :
            process.join()

        print("Finished setting up " + str(len(self.countries_dict)) + " countries!")
        
        #Setup dataframe for storing current state of everything, for easier access then looking up all values
        #This allows us to pass it as a value in the iterate_day_threaded function
        
        list_of_countries = sorted(self.countries_dict.keys())

        #Create a dataframe as a ledger to keep track of everything, mainly as a way to easily save the state of sim and export to a graph
        self.current_global_state = pd.DataFrame(columns=list_of_countries)

        print(self.current_global_state)




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




if __name__ == '__main__' :
    #Create simulator object
    #infection_rate, percent_social_distancing, government_action_timing, time_in_hospital, hospitalization_rate, mortality_rate
    sim = Virus_Simulator(.2, .7, .05, 14, .12, .2)

    '''
    Uncomment to create datafiles if missing
    Remember to download both cities list and gdp list from sources if doing so, and rename gdp file
    '''
    #sim.filter_raw_cities_datafile("Data\\worldcitiespop.csv)
    #sim.filter_raw_gdp_datafile("Data\\raw_gdp_capita.csv")
    #sim.filter_raw_beds_datafile("Data\\beds.csv")
    
    #Load all data
    sim.load_data()
    
    #Create countries and cities, and setup the world for the simulation
    sim.create_countries()
