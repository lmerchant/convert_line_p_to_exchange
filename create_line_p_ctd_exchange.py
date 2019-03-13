"""

Create ctd exchange file for Canadian cruises from 2007-01 to 2018-001

Will be assuming same ship code of 18DD for ship John P. Tully,
Can't use script unless modified for 2018-026 since ship is Sir Wilfrid Laurier


Get URL of cruise file listing all cruise lines and their data,
url: https://www.waterproperties.ca/linep/2017-01/donneesctddata/2017-01-ctd-cruise.csv

File headers:

File Name,
Zone,
FIL:START TIME YYYY/MM/DD, 
HH:MM,
LOC:EVENT_NUMBER,
LOC:LATITUDE,
LOC:LONGITUDE,
LOC:STATION,
INS:LOCATION,    

Pressure:CTD [dbar],
Temperature:CTD [deg_C_(ITS90)],
Salinity:CTD [PSS-78],
Sigma-t:CTD [kg/m^3],
Transmissivity:CTD [*/m],
Oxygen:Dissolved:CTD:Volume [ml/l],
Oxygen:Dissolved:CTD:Mass [µmol/kg],
Fluorescence:CTD:Seapoint [mg/m^3],
Fluorescence:CTD:Wetlabs [mg/m^3],
PAR:CTD [µE/m^2/sec] 



Read in all data lines into a data frame, then filter P line and sort.
Then group rows by P line and it's sequential event number to find castno

For cruise list, need to have corresponding expocodes in corresponding order



---------------------------------

Instructions:
-------------

Setup:
------
Folder structure required. In folder of script, mkdir line_p 

The line_p folder is where every expocode ctd file sets are stored


To Run:
-------

First, determine which cruises to get data for by setting them
in cruise_list and expocode_list

To run script, in virtual environment, type:
python get_line_p_data.py


Script Limits:
--------------
Currently, script not capable of noticing when data variable missing such 
as for cruise 2009-03 so this cruise can not be created as this script
is written.


Testing:
--------
To test the script, comment/uncomment the following lines in load_p_line_cruise 

    Comment this group reading from url ->
    # Read in csv file and split text on returns to get a list of lines
    txt = urlopen(url).read()
    decode_txt = txt.decode('windows-1252')
    raw_csv = decode_txt.split('\r\n')

    Uncomment this group reading from a test file ->
    # # For testing
    # text_file = open("./2017_test_file2.csv", "r")
    # txt = text_file.read()
    # raw_csv = txt.split('\n')


Renaming Variable Columns:
--------------------------
Make changes in 3 places since code is still hard coded in

rename_pline_columns, get_data_columns, and get_data_units


Removing Variable Columns without flag columns:
-----------------------------------------------
Make changes in 3 places:
rename_pline_columns, get_data_columns, and get_data_units

-> Important, keep order same in get_data_columns, and get_data_units


Removing Variable Columns with flag columns:
-----------------------------------------------
Make changes in 4 places:
rename_pline_columns, insert_flag_colums, get_data_columns, and get_data_units

-> Important, keep order same in get_data_columns, and get_data_units
-> Important, remove variable column and flag column lines together
   in insert_flag_colums





"""

import os
import pandas as pd
import numpy as np
from urllib.request import urlopen
import datetime
import csv


class RawData():

    def get_cruise_list(self):

        cruise_list = [
            ('2018', '001', '18LU20180218'),
            ('2018', '026', '18DD20180605'),
            ('2017', '01', '18DD20170205'),
            ('2017', '06', '18DD20170604'),
            ('2017', '08', '18DD20170815'),
            ('2016', '01', '18DD20160208'),
            ('2016', '06', '18DD20160605'),
            ('2016', '08', '18DD20160816'),
            ('2015', '01', '18DD20150210'),
            ('2015', '09', '18DD20150607'),
            ('2015', '10', '18DD20150818'),
            ('2014', '01', '18DD20140210'),
            ('2014', '18', '18DD20140608'),
            ('2014', '19', '18DD20140819'),
            ('2013', '01', '18DD20130205'),
            ('2013', '17', '18DD20130607'),
            ('2013', '18', '18DD20130820'),
            ('2012', '01', '18DD20120206'),
            ('2012', '12', '18DD20120522'),
            ('2012', '13', '18DD20120814'),
            ('2011', '01', '18DD20110208'),
            ('2011', '26', '18DD20110603'),
            ('2011', '27', '18DD20110816'),
            ('2010', '01', '18DD20100202'),
            ('2010', '13', '18DD20100605'),
            ('2010', '14', '18DD20100817'),
            ('2009', '03', '18DD20090127'),
            ('2009', '09', '18DD20090606'),
            ('2009', '10', '18DD20090818'),
            ('2008', '01', '18DD20080129'),
            ('2008', '26', '18DD20080528'),
            ('2008', '27', '18DD20080812'),
            ('2007', '01', '18DD20070207'),
            ('2007', '13', '18DD20070530'),
            ('2007', '15', '18DD20070814')
        ]

        return cruise_list        


    def build_url(self, year,month):
        url = 'https://www.waterproperties.ca/linep/' + year + '-' + month + '/donneesctddata/' + year + '-' + month + '-ctd-cruise.csv'
        return url


    def get_raw_csv(self, url):

        # Read in csv file and split text on returns to get a list of lines
        # txt = urlopen(url).read()
        # decode_txt = txt.decode('windows-1252')
        # raw_csv = decode_txt.split('\r\n')


        # # For testing
        # text_file = open("./2017_test_file2.csv", "r")
        #text_file = open("./test/data/2017_test_file2.csv", "r")
        #text_file = open("./test/data/2017-01-ctd-cruise.csv", "r")
        # txt = text_file.read()
        # raw_csv = txt.split('\n')

        # read csv file into a list
        test_filename = "./test/data/18DD20070207_2007-01-ctd-cruise.csv"

        with open(test_filename, newline='\n', encoding='windows-1252') as f:
            reader = csv.reader(f)
            raw_csv = list(reader)

        return raw_csv


    def load_p_line_cruise(self, url):

        # The url is a csv file
        # This cruise csv file lists all lines from from ctd files and all stations
        # Read in csv file, find header and save.
        # Then read in all data lines and put into a data frame
        # Filter data from for Line P stations, sort and return data frame

        # Sample parameter header and first data line
        # File Name,Zone,FIL:START TIME YYYY/MM/DD, HH:MM,LOC:EVENT_NUMBER,LOC:LATITUDE,LOC:LONGITUDE,LOC:STATION,INS:LOCATION,Pressure:CTD [dbar],Temperature:CTD [deg_C_(ITS90)],Salinity:CTD [PSS-78],Sigma-t:CTD [kg/m^3],Transmissivity:CTD [*/m],Oxygen:Dissolved:CTD:Volume [ml/l],Oxygen:Dissolved:CTD:Mass [µmol/kg],Fluorescence:CTD:Seapoint [mg/m^3],Fluorescence:CTD:Wetlabs [mg/m^3],PAR:CTD [µE/m^2/sec]
        #,,,,,,,,,,,,,,,,,,
        #2017-01-0001.ctd,UTC,06-02-17, 23:58,1,48.65883,-123.49934,SI,Mid-ship,1.1,6.6845,28.1863,22.1205,,,,1.186,,12.6  


        # Read in csv file and split text on returns to get a list of lines
        raw_csv = self.get_raw_csv(url)


        # Find header and lines containing values from ctd files

        comment_header = []

        count = 0
        for line in raw_csv:
            if '.ctd' not in line:
                # then line is a comment header
                # prepend a # sign
                line_comment = '#' + line
                comment_header.append(line_comment)
                count = count+1
            else:
                # Found ctd data line
                break

        # clean_csv contains all the data lines
        # Get data column header above comma separation line
        parameter_header = raw_csv[count-2].split(',')
        clean_csv = raw_csv[count:]

        # Get data lines into a list
        data = []
        for row in clean_csv:
            data.append(row.split(','))

        # Import data lines into a data frame
        df = pd.DataFrame(data,columns=parameter_header)

        # Get P-Line Stations only

        # drop any rows with NaN value in Pressure:CTD column
        # Do this because data sets separated by empty rows
        df.dropna(subset=['Pressure:CTD [dbar]'], inplace=True)

        # TODO. Not deleting empty rows. Fill empty values with NaN?
        print(df.head())

        # Find rows starting with P in the LOC:STATION column
        df = df[df['LOC:STATION'].str.startswith('P')]

        # Convert to numeric values
        df = df.apply(pd.to_numeric, errors='ignore')


        # # drop any rows with NaN value in Pressure:CTD column
        # # Do this because data sets separated by empty rows
        # df.dropna(subset=['Pressure:CTD [dbar]'], inplace=True)
        # print(df.head())


        # Sort by station and then pressure and reset index to match
        df.sort_values(by=['LOC:STATION','Pressure:CTD [dbar]'],inplace=True)
        df.reset_index(drop=True,inplace=True)

        return df, comment_header


class Headers():

    def __init__(self):
        self.params = Parameters()

    def create_column_headers(self, data_params):

        column_headers = []

        # Get data units from all columns
        data_units_dict = self.params.get_data_units(data_params)

        column_name_row = []
        column_units_row = []

        for key, value in data_units_dict.items():

            column_name_row.append(key)
            column_units_row.append(value)

        # Join rows with a comma
        name_row = ','.join(column_name_row)
        units_row = ','.join(column_units_row)

        column_headers.append(name_row)
        column_headers.append(units_row)

        return column_headers


    def create_metadata_header(self, data_set):

        metadata_header = []

        first_row = data_set.iloc[0]

        metadata_header.append('EXPOCODE = ' + first_row['EXPOCODE'])
        metadata_header.append('STNBR = ' + first_row['STATION'])
        metadata_header.append('CASTNO = ' + str(first_row['CASTNO']))
        metadata_header.append('DATE = ' + first_row['DATE'])
        metadata_header.append('TIME = ' + first_row['TIME'])
        metadata_header.append('LATITUDE = ' + str(first_row['LATITUDE']))
        metadata_header.append('LONGITUDE = ' + str(first_row['LONGITUDE']))

        number_headers = len(metadata_header) + 1
        number_headers_line = 'NUMBER_HEADERS = ' + str(number_headers)

        metadata_header.insert(0, number_headers_line)

        return metadata_header


class Parameters:

    def get_meta_params(self):
        
        params =[
        {'whpname' : 'DATE' , 'longname':'FIL:START TIME YYYY/MM/DD', 'units' : ''},                       
        {'whpname' : 'TIME' , 'longname':' HH:MM', 'units' : ''},
        {'whpname' : 'EVENT' , 'longname' : 'LOC:EVENT_NUMBER', 'units' : ''},                              
        {'whpname' : 'STATION' , 'longname':'LOC:STATION', 'units' : ''},                                  
        {'whpname' : 'LATITUDE' , 'longname':'LOC:LATITUDE', 'units' : ''},                               
        {'whpname' : 'LONGITUDE' , 'longname':'LOC:LONGITUDE', 'units' : ''},                             
        ]     

        return params


    def get_all_data_params(self):
        
        params =[                             
        {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},                          
        {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
        {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
        {'whpname' : 'CTDSAL' , 'longname':'Salinity:Practical:CTD [PSS-78]', 'units' : 'PSS-78'},
        {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},   
        {'whpname' : 'CTDXMISS' , 'longname':'Transmissivity:CTD [*/m]', 'units' : ''},
        {'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'},   
        {'whpname' : 'CTDFLUOR', 'longname': 'Fluorescence:CTD:Seapoint', 'units' : 'MG/M^3'},
        {'whpname' : 'CTDFLUOR', 'longname': 'Fluorescence:CTD [mg/m^3]', 'units' : 'MG/M^3'},
        {'whpname' : 'CTDFLUOR_TSG' , 'longname':'Fluorescence:CTD:Wetlabs [mg/m^3]', 'units' : 'MG/M^3'},
        {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}     
        ]        

        return params


    def get_data_params(self, df):

        column_params = []
        
        all_params = self.get_all_data_params()       

        for param in all_params:

            param_name = param['longname']

            if param_name in df.columns:
                column_params.append(param)

        return column_params


    def get_data_units(self, data_params):

        data_units_dict = {}

        for param in data_params:
            data_units_dict[param['whpname']] = param['units']

        return data_units_dict        


class DataColumns:

    def rename_pline_columns(self, df, meta_params, data_params):

        param_dict = {}

        for param in meta_params:
            param_dict[param['longname']] = param['whpname']    

        for param in data_params:
            param_dict[param['longname']] = param['whpname']

        df.rename(columns=param_dict, inplace=True)

        return df


    def insert_flag_colums(self, df, data_params):

        new_params = []

        # Get location of column to insert flag column after
        # Insert flag column with value of 2
        for param in data_params:

            param_name = param['whpname']
            param_loc = df.columns.get_loc(param_name)
            flag_name = '{}_FLAG_W'.format(param_name)

            df.insert(param_loc + 1, flag_name, 2)

            # Insert flag param dict into data_params list
            param_insert = {}
            param_insert['whpname'] = flag_name
            param_insert['longname'] = flag_name
            param_insert['units'] = ''

            new_params.append(param)
            new_params.append(param_insert)

        return df, new_params


    def reformat_date_column(self, df):

        # Reformat DATE column from dd-mm-yy to yyyymmdd
        pattern = r'(\d\d)-(\d\d)-(\d\d)'
        repl = r'20\3\2\1'

        df['DATE'] = df['DATE'].str.replace(pattern, repl)

        return df


    def insert_expocode_column(self, df, expocode):

        # Create EXPOCODE column

        STATION_LOC = df.columns.get_loc('STATION')

        # Insert before STATION column
        df.insert(STATION_LOC - 1, 'EXPOCODE', expocode)

        return df


    def insert_castno_column(self, df):

        # Create CASTNO column and fill with dummy value first
        # Insert after STATION column    
        STATION_LOC = df.columns.get_loc('STATION')
        df.insert(STATION_LOC + 1, 'CASTNO', 0)

        # castno is sequential number of indiv events at a single station
        # combine station and event #, get unique of this combination while including Station and Event columns
        # count events in station subsets

        # Unique subset of station and event

        # castno station event#
        # 1 P20  event 18
        # 2 P20  event 19
        # 3 P20  event 26

        # Sort by station and then event and reset index to match
        df.sort_values(by=['STATION','EVENT'],inplace=True)
        df.reset_index(drop=True,inplace=True)


        # Get unique values of STATION column
        station_df = df['STATION'].copy()
        unique_station_df = station_df.drop_duplicates()

        # Get list of unique stations
        unique_station_list = unique_station_df.tolist() 

        # For each station in list, get subset of main df
        # Find unique events, increment index of events found
        # then set that event in main df to castno calcuated

        for station in unique_station_list:

            # Get dataframe subset for station
            df_subset = df.loc[df['STATION'] == station].copy()
            df_subset.reset_index(drop=True,inplace=True)

            # Get unique events in for this station subset
            event_df = df_subset['EVENT'].copy()
            event_df.reset_index(drop=True,inplace=True)

            unique_event_df = event_df.drop_duplicates()
            unique_event_df.reset_index(drop=True,inplace=True)

            # Get list of unique events for station
            unique_event_list = unique_event_df.tolist() 

            # Use index of unique event list to creat CASTNO
            # since df was sorted on Event#

            # Events were sorted, so increasing castno corresponding to increasing event #
            for index, event in enumerate(unique_event_list):
                # index of unique event list gives the increasing # indicating a castno
                # Then for each event in df, set CASTNO value
                df.loc[df.EVENT == event, 'CASTNO'] = index + 1

        return df


    def insert_station_castno_column(self, df):

        # Create STATION_CAST column which is a combo of station and castno to 
        # make it easy to sort and group on this value for each station to save
        # each to a file
        df['STATION_CASTNO'] = df['STATION'].apply(str) + '_' + df['CASTNO'].apply(str)

        return df


    def get_data_columns(self, df, data_params):

        # Get list of columns from params
        col_list = []

        for param in data_params:

            col_list.append(param['whpname'])

        # Get params set of columns from df
        df = df[col_list].copy()

        return df


    def get_unique_station_castno(self, df):

        # Get unique values of STATION_CASTNO column
        station_castno_df = df['STATION_CASTNO'].copy()
        unique_station_castno_df = station_castno_df.drop_duplicates()
        unique_station_castno_df.reset_index(drop=True,inplace=True)

        return unique_station_castno_df


    def get_station_castno_df_sets(self, df, unique_station_castno_df):

        station_df_sets = []

        # Get list of unique station castno dataframes
        unique_station_castno_list = unique_station_castno_df.tolist()

        # Convert unique_station_castno_df to a list
        # For each number in list, get subset of df
        # and append to station_df_sets

        for station_castno in unique_station_castno_list:

            df_subset = df.loc[df['STATION_CASTNO'] == station_castno].copy()
            df_subset.reset_index(drop=True,inplace=True)

            station_df_sets.append(df_subset)

        return station_df_sets



class DataFile():

    def __init__(self):
        self.headers = Headers()
        self.data_columns = DataColumns()

    def create_start_end_lines(self):

        # first_row = data_set.iloc[0]
        # expocode = first_row['EXPOCODE']
        # start_line = 'CTD,' + expocode
        # end_line = 'END_DATA'
        # return start_line, end_line

        now = datetime.datetime.now()

        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')

        start_line = "CTD,{}{}{}CCHSIOLMM".format(year,month,day)

        end_line = 'END_DATA'

        return start_line, end_line


    def get_ctd_filename(self, data_set):

        first_row = data_set.iloc[0]

        expocode = first_row['EXPOCODE']
        stnbr = first_row['STATION']
        castno = first_row['CASTNO']

        str_stnbr = str(stnbr)
        str_castno = str(castno)

        # if stnbr has a slash in name, replace with a dash
        if '/' in str_stnbr:
            str_stnbr = str_stnbr.replace('/', '-')


        ctd_filename = './exchange_line_p_data/' + expocode + '_ct1/' + expocode + '_' + str_stnbr + '_' + str_castno + '_ct1.csv'

        return ctd_filename


    def write_data_to_file(self, station_castno_df_sets, comment_header, meta_params, data_params):

        # Write data sets to file

        # Get expocode to make directory for files
        first_row = station_castno_df_sets[0].iloc[0]
        expocode = first_row['EXPOCODE']

        # Make sub directory in './exchange_line_p_data'
        directory = './exchange_line_p_data/' + expocode + '_ct1'

        if not os.path.exists(directory):
            os.makedirs(directory)


        # Get file start and end lines
        #start_line, end_line = create_start_end_lines(station_castno_df_sets[0])
        #start_line, end_line = create_start_end_lines() 
        start_line, end_line = self.create_start_end_lines()

        # Create column and data units lines
        #column_headers = create_column_headers(data_params)
        column_headers = self.headers.create_column_headers(data_params)

        # Loop over unique row sets (STATION and CASTNO)        

        for data_set in station_castno_df_sets:

            # Get filename
            #ctd_filename = get_ctd_filename(data_set)
            ctd_filename = self.get_ctd_filename(data_set)

            # Create metadata headers
            # Since all the same, use first data set
            #metadata_header = create_metadata_header(data_set)
            metadata_header = self.headers.create_metadata_header(data_set)        

            # Get data columns
            #data_columns_df = get_data_columns(data_set, data_params)
            data_columns_df = self.data_columns.get_data_columns(data_set, data_params)

            # Write to file

            data_columns_df.to_csv(ctd_filename, sep=',', index=False,header=False, encoding='utf-8')

            with open(ctd_filename, 'r') as original: data = original.read()

            # Create string to prepend
            prepend_string = ''
            comment_header_string = ''
            metadata_header_string = ''
            column_header_string = ''

            start_line_str = start_line + '\n'

            for header in comment_header:
                comment_header_string = comment_header_string + header + '\n' 

            for header in metadata_header:
                metadata_header_string = metadata_header_string + header + '\n'   

            for header in column_headers:
                column_header_string = column_header_string + header + '\n' 

            prepend_string = start_line_str + comment_header_string + metadata_header_string + column_header_string

            # Prepend ctd file
            with open(ctd_filename, 'w') as modified: modified.write(prepend_string + data)

            # Append ctd file
            with open(ctd_filename, 'a') as f:
                f.write("{}\n".format(end_line))




def main():

    raw_data = RawData()
    params = Parameters()    
    data_columns = DataColumns()
    data_file = DataFile()

    cruise_list = raw_data.get_cruise_list()

    for cruise in cruise_list:

        # Get URL of cruise information
        # url: https://www.waterproperties.ca/linep/2017-01/donneesctddata/2017-01-ctd-cruise.csv
        url = raw_data.build_url(cruise[0],cruise[1])

        # Get dataframe holding all Station P data lines
        df, comment_header = raw_data.load_p_line_cruise(url)

        # Get params
        meta_params = params.get_meta_params()
        data_params = params.get_data_params(df)

        # Rename all data columns 
        df = data_columns.rename_pline_columns(df, meta_params, data_params)

        # Insert flag columns
        df, data_params = data_columns.insert_flag_colums(df, data_params)

        # Reformat date column
        df = data_columns.reformat_date_column(df)

        # Get expocode
        expocode = cruise[2]

        # Insert expocode column
        df = data_columns.insert_expocode_column(df, expocode)

        # Insert castno column
        df = data_columns.insert_castno_column(df)

        # Insert station_castno column to get unique CTD files
        df = data_columns.insert_station_castno_column(df)
    

        # Get unique station_castno sets
        unique_station_castno_sets = data_columns.get_unique_station_castno(df)

        # Get data sets from dataframe for unique station and castno
        station_castno_df_sets = data_columns.get_station_castno_df_sets(df, unique_station_castno_sets)

        data_file.write_data_to_file(station_castno_df_sets, comment_header, meta_params, data_params)

        print("Completed " + expocode)

        break



if __name__ == '__main__':

  main()
