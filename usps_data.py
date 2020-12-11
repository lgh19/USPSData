import pandas as pd
import numpy as np
import math

class USPSData:
    
    fileName = ""
    global df

    def __init__(self, fileName):
        self.df = pd.DataFrame(data=None, columns=None)
        self.fileName = fileName
        self.df = pd.read_csv(fileName)
        print(self.df)
        #Total address counts (residential, business, other)
        self.df['TOTAL_ADDRESS_COUNT'] = 0
        #Total vacant (residential, business, other)
        self.df['TOTAL_VAC_COUNT'] = 0
        #Average days vacant (residential, business)
        self.df['TOTAL_AVG_VAC'] = 0
        #No-stat (residential, business, other) – no-stat addresses are those that the USPS deems not ready for occupancy, such as long-vacant   properties, or those under construction.
        self.df['TOTAL_NO_STAT'] = 0
        #Previous quarter vacant and no-stat currently in-service
        self.df['TOTAL_PQV_PQNS_IS'] = 0

    

    def addColumns(self):
        def isNaN(string):
            return string != string
    
    
    
        for index, row in self.df.iterrows():
                temp_ams_res = row.AMS_RES
                temp_ams_bus = row.AMS_BUS
                temp_ams_oth = row.AMS_OTH
                total_addresses = temp_ams_res + temp_ams_bus + temp_ams_oth
                self.df.at[index, 'TOTAL_ADDRESS_COUNT'] = total_addresses
                temp_res_vac = row.RES_VAC
                temp_bus_vac = row.BUS_VAC
                temp_oth_vac = row.OTH_VAC
                total_vacant = temp_res_vac + temp_bus_vac + temp_oth_vac
                self.df.at[index, 'TOTAL_VAC_COUNT'] = total_vacant
                temp_pqns_is_r = row.PQNS_IS_R
                temp_pqns_is_b = row.PQNS_IS_B
                temp_pqns_is_o = row.PQNS_IS_O
                temp_pqv_is_res = row.PQV_IS_RES
                temp_pqv_is_bus = row.PQV_IS_BUS
                temp_pqv_is_oth = row.PQV_IS_OTH
                total_pqns_is = temp_pqns_is_r + temp_pqns_is_b + temp_pqns_is_o
                total_pqv_is = temp_pqv_is_res + temp_pqv_is_bus + temp_pqv_is_oth
                total_is = total_pqns_is + total_pqv_is
                self.df.at[index, 'TOTAL_PQV_PQNS_IS']= total_is
                temp_ns_res = row.NOSTAT_RES
                temp_ns_bus = row.NOSTAT_BUS
                temp_ns_oth = row.NOSTAT_OTH
                total_nostat = temp_ns_res + temp_ns_bus + temp_ns_oth
                self.df.at[index, 'TOTAL_NO_STAT'] = total_nostat
                temp_avg_v_r = row.AVG_VAC_R
                temp_avg_v_b = row.AVG_VAC_B
                total_avg = temp_avg_v_r + temp_avg_v_b
                self.df.at[index, 'TOTAL_AVG_VAC'] = total_avg


            return self.df

    def writeToFile(self):
        userInput = raw_input("output file name: ")
        userInput = str(userInput)
        userInput += ".csv"
        self.df.to_csv(userInput)
        print("output file: "),
        print(userInput)


   
    #create an object of USPSData class with the file you want to add columns to
    a=USPSData('USPS_09_2014.csv')
    
    #call the addColumns() method on the class to add the columns
    a.addColumns()
    
    #call the writeToFile() method to write data with new columns to an output excel doc
    #it will ask for you to name the file (and .csv is automatically added to the end)
    a.writeToFile()
