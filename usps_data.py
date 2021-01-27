#example of how to run the code:
        
#create an object of the class (a) and pass in 2 csv files, the first being a quarterly file and the second being the file with all quarters aggregated in one file
a = USPSData('USPS_09_2014.csv', 'USPSVacancyData.csv)
#^this will run the entire code and the output will be a new quarterly csv file with only the Pennsylvania tracts and the new columns added, 
#and a second csv file that has all the previous quarterly data and this new quarter's data appended to it 
   


import pandas as pd
import numpy as np
import math

class USPSData:
    fileName = ""
    allPghFileName = ""
    global df
    global pghdf
    global endFileName
    global file
    global finalPghFile
    global quarter
    
    def __init__(self, fileName, allPghFileName):
        self.endFileName = str(fileName)
        a = self.endFileName.split('.')
        self.endFileName = a[0]
        self.file = str(fileName)
        s = self.file.split('_')
        self.quarter = s[2]
        self.df = pd.DataFrame(data=None, columns=None)
        self.fileName = fileName
        self.df = pd.read_csv(fileName)
        self.df.columns= self.df.columns.str.upper()
        #print(self.df)
        self.pghdf = pd.read_csv(allPghFileName)
        self.finalPghFile = str(allPghFileName)
        
        # Total address counts (residential, business, other)
        self.df['TOTAL_ADDRESS_COUNT'] = 0
        # Total vacant (residential, business, other)
        self.df['TOTAL_VAC_COUNT'] = 0
        # Average days vacant (residential, business)
        self.df['TOTAL_AVG_VAC'] = 0
        # No-stat (residential, business, other) – no-stat addresses are those that the USPS deems not ready for occupancy, such as long-vacant   properties, or those under construction.
        self.df['TOTAL_NO_STAT'] = 0
        # Previous quarter vacant and no-stat currently in-service
        self.df['TOTAL_PQV_PQNS_IS'] = 0
        self.df['QUARTER'] = self.quarter
        self.df['PITT_TRACT'] = ''
        self.df = self.addColumns()
        self.df = self.writeToFile()

    def addColumns(self):
        def isNaN(string):
            return string != string
        for index, row in self.df.iterrows():
            tempgeoid = row.GEOID
            tempgeoid = str(tempgeoid)
            if tempgeoid.startswith('42'):
                self.df.at[index, 'PITT_TRACT'] = True
                #print("true")
                
        self.df = self.df.loc[(self.df['PITT_TRACT'] == True)]
        
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
            self.df.at[index, 'TOTAL_PQV_PQNS_IS'] = total_is
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
        for index, row in self.df.iterrows():
            tempquarter = row.QUARTER
            tempquarter = str(tempquarter)
            if '/' in tempquarter:
                #print("Passing: "),
                #print(tempquarter)
                pass
            else:
                a = list(tempquarter)
                #print(a)
                #print(a[0])
                if (len(a)) == 5:
                    newQuarter = a[0] + '/' + a[1] + a[2] + a[3] + a[4]
                    newQuarter = str(newQuarter)
                    #print(newQuarter)
                elif len(a) == 6:
                    newQuarter = a[0] + a[1] + '/' + a[2] + a[3] + a[4] + a[5]
                    newQuarter = str(newQuarter)
                    #print(newQuarter)
                self.df.at[index, 'QUARTER'] = newQuarter
                
        self.pghdf = self.pghdf.append(self.df)
        b = self.endFileName + "Penn_Only_and_Columns_Added.csv"
        self.df.to_csv(b)
        c = self.finalPghFile
        c = str(c)
        c = "Added_Quarter_" + self.quarter+"_Total"+c
        self.pghdf.to_csv(c)
        print("quarter output file: "),
        print(b)
        print("Total Pennsylvania Tracts file: "),
        print(c)
        
