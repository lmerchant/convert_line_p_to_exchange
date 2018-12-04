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


# Formatting error, so not run with this program
# Run for one cruise
cruise_list =[ 
    ('2009', '03')
]    

expocode_list = [
    '18DD20090818'
]

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


def build_url(year,month):
    url = 'https://www.waterproperties.ca/linep/' + year + '-' + month + '/donneesctddata/' + year + '-' + month + '-ctd-cruise.csv'
    return url


def load_p_line_cruise(url):

    # The url is a csv file
    # This cruise csv file lists all lines from from ctd files and all stations
    # Read in csv file, find header and save.
    # Then read in all data lines and put into a data frame
    # Filter data from for Line P stations, sort and return data frame

    # Sample header and first data line
    # File Name,Zone,FIL:START TIME YYYY/MM/DD, HH:MM,LOC:EVENT_NUMBER,LOC:LATITUDE,LOC:LONGITUDE,LOC:STATION,INS:LOCATION,Pressure:CTD [dbar],Temperature:CTD [deg_C_(ITS90)],Salinity:CTD [PSS-78],Sigma-t:CTD [kg/m^3],Transmissivity:CTD [*/m],Oxygen:Dissolved:CTD:Volume [ml/l],Oxygen:Dissolved:CTD:Mass [µmol/kg],Fluorescence:CTD:Seapoint [mg/m^3],Fluorescence:CTD:Wetlabs [mg/m^3],PAR:CTD [µE/m^2/sec]
    #,,,,,,,,,,,,,,,,,,
    #2017-01-0001.ctd,UTC,06-02-17, 23:58,1,48.65883,-123.49934,SI,Mid-ship,1.1,6.6845,28.1863,22.1205,,,,1.186,,12.6  


    # # Read in csv file and split text on returns to get a list of lines
    # txt = urlopen(url).read()
    # decode_txt = txt.decode('windows-1252')
    # raw_csv = decode_txt.split('\r\n')


    # For testing
    text_file = open("./2017_test_file2.csv", "r")
    txt = text_file.read()
    raw_csv = txt.split('\n')

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
    header = raw_csv[count-2].split(',')
    clean_csv = raw_csv[count:]

    # Get data lines into a list
    data = []
    for row in clean_csv:
        data.append(row.split(','))

    # Import data lines into a data frame
    df = pd.DataFrame(data,columns=header)


    # Get P-Line Stations only

    # drop any rows with NaN value in Pressure:CTD column
    # Do this because data sets separated by empty rows
    df.dropna(subset=['Pressure:CTD [dbar]'], inplace=True)

    # Find rows starting with P in the LOC:STATION column
    df = df[df['LOC:STATION'].str.startswith('P')]

    # Convert to numeric values
    df = df.apply(pd.to_numeric, errors='ignore')

    # Sort by station and then pressure and reset index to match
    df.sort_values(by=['LOC:STATION','Pressure:CTD [dbar]'],inplace=True)
    df.reset_index(drop=True,inplace=True)

    return df, comment_header


def get_params():
    
    params =[
    {'whpname' : 'DATE' , 'longname':'FIL:START TIME YYYY/MM/DD', 'units' : ''},                       
    {'whpname' : 'TIME' , 'longname':' HH:MM', 'units' : ''},
    {'whpname' : 'EVENT' , 'longname' : 'LOC:EVENT_NUMBER', 'units' : ''},                              
    {'whpname' : 'STATION' , 'longname':'LOC:STATION', 'units' : ''},                                  
    {'whpname' : 'LATITUDE' , 'longname':'LOC:LATITUDE', 'units' : ''},                               
    {'whpname' : 'LONGITUDE' , 'longname':'LOC:LONGITUDE', 'units' : ''},                             
    {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},                          
    {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
    {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
    {'whpname' : 'CTDXMISS' , 'longname':'Transmissivity:CTD [*/m]', 'units' : ''},
    {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},
    {'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'},   
    {'whpname' : 'CTDFLUOR_TSG' , 'longname':'Fluorescence:CTD:Wetlabs [mg/m^3]', 'units' : 'MG/M^3'},
    {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}   
    ]     

    return params


def rename_pline_columns(df, params):

    #{df.rename(columns={param['longname']: param['whpname']}, inplace=True) for param in params}

    param_dict = {}

    for param in params:
        param_dict[param['longname']] = param['whpname']

    print(param_dict)

    df.rename(columns=param_dict, inplace=True)

    print(df.head())



    #df.rename(columns={'FIL:START TIME YYYY/MM/DD' : 'DATE', ' HH:MM': 'TIME', 'LOC:EVENT_NUMBER':'EVENT', 'LOC:STATION': 'STATION', 'LOC:LATITUDE':'LATITUDE', 'LOC:LONGITUDE':'LONGITUDE','Pressure:CTD [dbar]':'CTDPRS','Temperature:CTD [deg_C_(ITS90)]':'CTDTMP', 'Salinity:CTD [PSS-78]': 'CTDSAL', 'Transmissivity:CTD [*/m]': 'CTDXMISS', 'Oxygen:Dissolved:CTD:Mass [µmol/kg]': 'CTDOXY', 'Fluorescence:CTD:Seapoint [mg/m^3]':'CTDFLUOR', 'Sigma-t:CTD [kg/m^3]': 'Sigma-t:CTD', 'Oxygen:Dissolved:CTD:Volume [ml/l]': 'Oxygen:Dissolved:CTD:Volume', 'Fluorescence:CTD:Wetlabs [mg/m^3]': 'Fluorescence:CTD:Wetlabs', 'PAR:CTD [µE/m^2/sec]': 'PAR:CTD'}, inplace=True)

    return df


def insert_flag_colums(df):

    # Get location of column to insert flag column after
    # Insert flag column with value of 2
    CTDPRS_LOC = df.columns.get_loc('CTDPRS')
    df.insert(CTDPRS_LOC + 1, 'CTDPRS_FLAG_W', 2)
    
    CTDTMP_LOC = df.columns.get_loc('CTDTMP')
    df.insert(CTDTMP_LOC + 1, 'CTDTMP_FLAG_W', 2)
    
    CTDSAL_LOC = df.columns.get_loc('CTDSAL')
    df.insert(CTDSAL_LOC + 1, 'CTDSAL_FLAG_W', 2)
    
    CTDOXY_LOC = df.columns.get_loc('CTDOXY')
    df.insert(CTDOXY_LOC + 1, 'CTDOXY_FLAG_W', 2)
    
    CTDXMISS_LOC = df.columns.get_loc('CTDXMISS')
    df.insert(CTDXMISS_LOC + 1, 'CTDXMISS_FLAG_W', 2)
    
    CTDFLUOR_LOC = df.columns.get_loc('CTDFLUOR')
    df.insert(CTDFLUOR_LOC + 1, 'CTDFLUOR_FLAG_W', 2)

    return df


def insert_flag_colums_at_end(df):

    # Add flag columns to end of df
    # Insert flag column with value of 2

    df = df.assign(CTDPRS_FLAG_W = 2)
    df = df.assign(CTDTMP_FLAG_W = 2)   
    df = df.assign(CTDSAL_FLAG_W = 2)
    df = df.assign(CTDOXY_FLAG_W = 2) 
    df = df.assign(CTDXMISS_FLAG_W = 2)
    df = df.assign(CTDFLUOR_FLAG_W = 2)   

    return df


def reformat_date_column(df):

    # Reformat DATE column from dd-mm-yy to yyyymmdd
    pattern = r'(\d\d)-(\d\d)-(\d\d)'
    repl = r'20\3\2\1'

    df['DATE'] = df['DATE'].str.replace(pattern, repl)

    return df


def get_expocode(index, expocode_list):

    expocode = expocode_list[index]

    return expocode


def insert_expocode_column(df, expocode):

    # Create EXPOCODE column

    STATION_LOC = df.columns.get_loc('STATION')

    # Insert before STATION column
    df.insert(STATION_LOC - 1, 'EXPOCODE', expocode)

    return df


def insert_castno_column(df):

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


def insert_station_castno_column(df):

    # Create STATION_CAST column which is a combo of station and castno to 
    # make it easy to sort and group on this value for each station to save
    # each to a file
    df['STATION_CASTNO'] = df['STATION'].apply(str) + '_' + df['CASTNO'].apply(str)

    return df


def get_unique_station_castno(df):

    # Get unique values of STATION_CASTNO column
    station_castno_df = df['STATION_CASTNO'].copy()
    unique_station_castno_df = station_castno_df.drop_duplicates()
    unique_station_castno_df.reset_index(drop=True,inplace=True)

    return unique_station_castno_df


def get_station_castno_df_sets(df, unique_station_castno_df):

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


def get_data_columns(df):

    # df = df[[
    #         'CTDPRS', 'CTDPRS_FLAG_W', 
    #         'CTDTMP', 'CTDTMP_FLAG_W',
    #         'CTDSAL', 'CTDSAL_FLAG_W',
    #         'CTDOXY', 'CTDOXY_FLAG_W',
    #         'CTDXMISS', 'CTDXMISS_FLAG_W',
    #         'CTDFLUOR', 'CTDFLUOR_FLAG_W',
    #         'Sigma-t:CTD', 'Oxygen:Dissolved:CTD:Volume',
    #         'Fluorescence:CTD:Wetlabs', 'PAR:CTD'
    #         ]].copy()

    df = df[[
            'CTDPRS', 'CTDPRS_FLAG_W', 
            'CTDTMP', 'CTDTMP_FLAG_W',
            'CTDSAL', 'CTDSAL_FLAG_W',
            'CTDOXY', 'CTDOXY_FLAG_W',
            'CTDXMISS', 'CTDXMISS_FLAG_W',
            'CTDFLUOR', 'CTDFLUOR_FLAG_W'
            ]].copy()

    return df


def get_metadata_columns(df):

    df = df[[
            'EXPOCODE',
            'STATION',
            'CASTNO',
            'DATE',
            'TIME',
            'LATITUDE',
            'LONGITUDE'
         ]].copy()

    return df


def create_metadata_header(data_set):

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


def get_data_units(params):

    # In column names, units are in []

    # Pressure:CTD [dbar]
    # Temperature:CTD [deg_C_(ITS90)]
    # Salinity:CTD [PSS-78]
    # Transmissivity:CTD [*/m]
    # Oxygen:Dissolved:CTD:Mass [µmol/kg]
    # Fluorescence:CTD:Seapoint [mg/m^3]

    # Sigma-t:CTD [kg/m^3],
    # Oxygen:Dissolved:CTD:Volume [ml/l]
    # Fluorescence:CTD:Wetlabs [mg/m^3],
    # PAR:CTD [µE/m^2/sec]


    data_units = {}

    data_units['CTDPRS'] = 'DBAR'
    data_units['CTDPRS_FLAG_W'] = ''
    data_units['CTDTMP'] = 'ITS-90'
    data_units['CTDTMP_FLAG_W'] = ''
    data_units['CTDSAL'] = 'PSS-78'
    data_units['CTDSAL_FLAG_W'] = ''
    data_units['CTDOXY'] = 'UMOL/KG'
    data_units['CTDOXY_FLAG_W'] = ''
    data_units['CTDXMISS'] = '*/M'
    data_units['CTDXMISS_FLAG_W'] = '' 
    data_units['CTDFLUOR'] = 'MG/M^3'   
    data_units['CTDFLUOR_FLAG_W'] = ''

    # data_units['Sigma-t:CTD'] = 'kg/m^3'
    # data_units['Oxygen:Dissolved:CTD:Volume'] = 'ml/l'
    # data_units['Fluorescence:CTD:Wetlabs'] = 'mg/m^3'
    # data_units['PAR:CTD'] = 'µE/m^2/sec'


    # print(data_units)

    # put params into a dictionary
    # {'CTDPRS': 'DBAR', 'CTDTMP': 'ITS-90', 'CTDSAL': 'PSS-78', 'CTDXMISS': '*/M', 'CTDFLUOR': 'MG/M^3', 'CTDPRS_FLAG_W': '', 'CTDTMP_FLAG_W': '', 'CTDSAL_FLAG_W': '', 'CTDOXY_FLAG_W': '', 'CTDXMISS_FLAG_W': '', 'CTDFLUOR_FLAG_W': ''}

    params_to_exclude = [
    'DATE',
    'TIME',
    'EVENT',
    'STATION',
    'LATITUDE',
    'LONGITUDE'
    ]

    flags_to_include = [
    'CTDPRS_FLAG_W',
    'CTDTMP_FLAG_W',
    'CTDSAL_FLAG_W',
    'CTDOXY_FLAG_W',
    'CTDXMISS_FLAG_W',
    'CTDFLUOR_FLAG_W'
    ]

    data_units_dict = {}

    for param in params:
        if param['whpname'] not in params_to_exclude:
            data_units_dict[param['whpname']] = param['units']

    for flag in flags_to_include:
        data_units_dict[flag] = ''

    print(data_units_dict)   


    return data_units, data_units_dict


def create_column_headers(params):

    column_headers = []

    # Get data units from all columns
    # At the moment, it is hard coded in and in correct order as
    # data columns pulled in through get_data_columns
    data_units, data_units_dict = get_data_units(params)   

    column_name_row = []
    column_units_row = []

    for key, value in data_units.items():

        column_name_row.append(key)
        column_units_row.append(value)

    # Join rows with a comma
    name_row = ','.join(column_name_row)
    units_row = ','.join(column_units_row)

    column_headers.append(name_row)
    column_headers.append(units_row)

    return column_headers


def create_start_end_lines(data_set):

    first_row = data_set.iloc[0]

    expocode = first_row['EXPOCODE']

    start_line = 'CTD,' + expocode

    end_line = 'END_DATA'

    return start_line, end_line


def get_ctd_filename(data_set):

    first_row = data_set.iloc[0]

    expocode = first_row['EXPOCODE']
    stnbr = first_row['STATION']
    castno = first_row['CASTNO']

    ctd_filename = './line_p/' + expocode + '_ct1/' + expocode + '_' + str(stnbr) + '_' + str(castno) + '_ct1.csv'

    return ctd_filename


def write_data_to_file(station_castno_df_sets, comment_header, params):

    # Write data sets to file

    # Get expocode to make directory for files
    first_row = station_castno_df_sets[0].iloc[0]
    expocode = first_row['EXPOCODE']

    # Make sub directory in './line_p'
    directory = './line_p/' + expocode + '_ct1'

    if not os.path.exists(directory):
        os.makedirs(directory)


    # Get file start and end lines
    start_line, end_line = create_start_end_lines(station_castno_df_sets[0]) 

    # Create column and data units lines
    column_headers = create_column_headers(params)


    # Loop over unique row sets (STATION and CASTNO)

    for data_set in station_castno_df_sets:

        # Get filename
        ctd_filename = get_ctd_filename(data_set)

        # Create metadata headers
        # Since all the same, use first data set
        metadata_header = create_metadata_header(data_set)        

        # Get data columns
        data_columns_df = get_data_columns(data_set)


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

    # ('2009', '03') omitted because format is different. There is no CTDSAL column
    # and this program doesn't take that into account    

    # -> 33 cruises in order given on cruise menu url 
    #https://waterproperties.ca/linep/cruises.php#cruises

    # cruise_list =[ 
    #     ('2018','001'),
    #     ('2017', '01'), ('2017','06'), ('2017','08'),
    #     ('2016', '01'), ('2016','06'), ('2016','08'),
    #     ('2015', '01'), ('2015', '09'), ('2015', '10'),
    #     ('2014', '01'), ('2014', '18'), ('2014', '19'),
    #     ('2013', '01'), ('2013', '17'), ('2013', '18'),
    #     ('2012', '01'), ('2012', '12'), ('2012', '13'),
    #     ('2011', '01'), ('2011', '26'), ('2011', '27'),
    #     ('2010', '01'), ('2010', '13'), ('2010', '14'),
    #     ('2009', '09'), ('2009', '10'),
    #     ('2008', '01'), ('2008', '26'), ('2008', '27'),
    #     ('2007', '01'), ('2007', '13'), ('2007', '15'),
    # ]


# Not including ('2009', '03') in cruise_list, so 
# no corresponding expocode for it, 18DD20090818


    # -> 33 corresponding expocodes

    # expocode_list = [
        # 18LU20180218
        # 18DD20170205
        # 18DD20170604
        # 18DD20170815
        # 18DD20160208
        # 18DD20160605
        # 18DD20160816
        # 18DD20150210
        # 18DD20150607
        # 18DD20150818
        # 18DD20140210
        # 18DD20140608
        # 18DD20140819
        # 18DD20130205
        # 18DD20130607
        # 18DD20130820
        # 18DD20120206
        # 18DD20120522
        # 18DD20120814
        # 18DD20110208
        # 18DD20110603
        # 18DD20110816
        # 18DD20100202
        # 18DD20100605
        # 18DD20100803
        # 18DD20090127
        # 18DD20090606
        # 18DD20080129
        # 18DD20080528
        # 18DD20070207
        # 18DD20070530
        # 18DD20070814
    # ]


    # # Formatting error, so can't run with this program as is
    # # Run for one cruise
    # cruise_list =[ 
    #     ('2009', '03')
    # ]    

    # expocode_list = [
    #     '18DD20090818'
    # ]


    # Run for one cruise
    cruise_list =[ 
        ('2017', '01')
    ]    

    expocode_list = [
        '18DD20070207'
    ]




    # Get index of loop to find corresponding expocode
    #for cruise in cruise_list:
    for index, cruise in enumerate(cruise_list):

        # Get URL of cruise information
        # url: https://www.waterproperties.ca/linep/2017-01/donneesctddata/2017-01-ctd-cruise.csv
        url = build_url(cruise[0],cruise[1])

        # Get dataframe holding all Station P data lines
        df, comment_header = load_p_line_cruise(url)

        # Get params
        params = get_params()

        # Rename all data columns 
        df = rename_pline_columns(df, params)

        # Insert flag columns
        df = insert_flag_colums(df)

        df1 = df.copy()
        df1 = insert_flag_colums_at_end(df1)

        # Reformat date column
        df = reformat_date_column(df)

        # Get expocode
        expocode = get_expocode(index, expocode_list)

        # Insert expocode column
        df = insert_expocode_column(df, expocode)

        # Insert castno column
        df = insert_castno_column(df)

        # Insert station_castno column to get unique CTD files
        df = insert_station_castno_column(df)
    
        # Get unique station_castno sets
        unique_station_castno_sets = get_unique_station_castno(df)

        # Get data sets from dataframe for unique station and castno
        station_castno_df_sets = get_station_castno_df_sets(df, unique_station_castno_sets)

        write_data_to_file(station_castno_df_sets, comment_header, params)



if __name__ == '__main__':

  main()
