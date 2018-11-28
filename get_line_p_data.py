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




TODO:
Looks like file https://www.waterproperties.ca/linep/2017-01/donneesctddata/2017-01-ctd-doc.csv,
contains just the P line data 


Step 2,
Read in all data lines into a data frame, then filter, sort, and extract on P line,
Save each extracted data frame to a file


TODO:

For each cruise, expand to extract data frames for all lines P1 to P26 

The data lines list the name of the file on each line, metadata, and data

So can create new column for cast # and station #

For composing cast # and station #, 

A cast # is the concatenation of the Event # and the P line. e.g. 16P3,
A station # is the P line, so for P3, it would be 3

Currently, dataframes are split on P line #. Could split on cast #


Event #
Filename: 2017-01-0001.ctd where the 0001 portion represents Event 1 as,
seen in the table on the cruise page https://waterproperties.ca/linep/2017-01/index.php

The event number is also listed in the column: LOC:EVENT_NUMBER


TODO:

Would have to extract metadata to create an exchange header, then take remaining dataframe,
columns and save as the data section with renamed columns. Would also need to count,
the header lines and add end data line to bottom


"""


"""
Cruises to get data for

2018 001,
2017 01 06 08,
2016 01 06 08,
2015 01 09 10,
2014 01 18 19,
2013 01 17 18,
2012 01 12 13,
2011 01 26 27,
2010 01 13 14,
2009 03 09 10,
2008 01 26 27,
2007 01 13 15


#2009 03 is omitted due to formatting error
# Have to process this separately

I'm not sure what formatting error it is

"""


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


    # Read in csv file and split text on returns to get a list of lines
    txt = urlopen(url).read()
    decode_txt = txt.decode('windows-1252')
    raw_csv = decode_txt.split('\r\n')

    # Find header and lines containing values from ctd files
    count = 0
    for line in raw_csv:
        if '.ctd' not in line:
            count = count+1
        else:
            break

    # clean_csv contains all the data lines
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
    df.dropna(subset=['Pressure:CTD [dbar]'], inplace=True)

    # Find rows starting with P in the LOC:STATION column
    df = df[df['LOC:STATION'].str.startswith('P')]

    # Convert to numeric values
    df = df.apply(pd.to_numeric, errors='ignore')

    # Sort by station and then pressure and reset index to match
    df.sort_values(by=['LOC:STATION','Pressure:CTD [dbar]'],inplace=True)
    df.reset_index(drop=True,inplace=True)

    return df


def rename_pline_columns(df):

    df.rename(columns={'FIL:START TIME YYYY/MM/DD' : 'DATE', 'HH:MM': 'TIME', 'LOC:EVENT_NUMBER':'EVENT', 'LOC:STATION': 'STATION', 'LOC:LATITUDE':'LATITUDE', 'LOC:LONGITUDE':'LONGITUDE','Pressure:CTD [dbar]':'CTDPRS','Temperature:CTD [deg_C_(ITS90)]':'CTDTMP', 'Salinity:CTD [PSS-78]': 'CTDSAL', 'Transmissivity:CTD [*/m]': 'CTDXMISS', 'Oxygen:Dissolved:CTD:Mass [µmol/kg]': 'CTDOXY', 'Fluorescence:CTD:Seapoint [mg/m^3]':'CTDFLUOR'}, inplace=True)

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


def get_data_columns(df):

    df = df[[
            'CTDPRS', 'CTDPRS_FLAG_W', 
            'CTDTMP', 'CTDTMP_FLAG_W',
            'CTDSAL', 'CTDSAL_FLAG_W',
            'CTDOXY', 'CTDOXY_FLAG_W',
            'CTDXMISS', 'CTDXMISS_FLAG_W',
            'CTDFLUOR', 'CTDFLUOR_FLAG_W'
            ]].copy()

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

    # Create CASTNO column and fill with dummy value
    # Insert after STATION column    
    STATION_LOC = df.columns.get_loc('STATION')
    df.insert(STATION_LOC + 1, 'CASTNO', 0)

    # castno is sequential number of indiv events at a single station

    # combine station and event #, get unique of this combination while including Station and Event columns

    # Now need to count events in station subsets

    # Unique subset of station and event

    # 1 P20  event 18
    # 2 P20  event 19
    # 3 P20  event 26

    # Sort by station and then event and reset index to match
    df.sort_values(by=['STATION','EVENT'],inplace=True)
    df.reset_index(drop=True,inplace=True)

    # Get unique values of STATION column
    station_df = df['STATION']

    unique_station_df = station_df.drop_duplicates()

    # Get list of unique dataframes
    unique_station_list = unique_station_df.tolist() 


    data_row_sets = []

    # For each number in list, get subset of df
    # and append to data_row_sets

    for station in unique_station_list:

        df_subset = df.loc[df['STATION'] == station].copy()

        # Get unique events in this subset
        event_df = df_subset['EVENT'].copy()

        unique_event_df = event_df.drop_duplicates()

        # Get list of unique dataframes
        unique_event_list = unique_event_df.tolist() 

        # Use index of unique event list to creat CASTNO

        for index, event in enumerate(unique_event_list):
            # For event in main df, set CASTNO at that event

            df_subset.loc[df.EVENT == event, 'CASTNO'] = index + 1

        # Add subset to row sets list
        data_row_sets.append(df_subset)

    # Expand all the data sets back into one dataframe
    df = pd.concat(data_row_sets)

    return df


def insert_station_castno_column(df):

    # Create STATION_CAST column

    df['STATION_CASTNO'] = df['STATION'].apply(str) + '_' + df['CASTNO'].apply(str)


    return df


def get_metadata_columns(df):

    # DATE 
    # TIME
    # EVENT
    # LATITUDE
    # LONGITUDE
    # STATION

    # EXPOCODE
    # STNBR
    # CASTNO

    pass


def get_metadata_row(df):

    pass


def create_metadata_dict(df):

    # create dict from metadata row
    # metadata_row = get_metadata_row(df)

    # include NUMBER_HEADERS to dict

    pass


def create_top_line(expocode):

    # create line CTD,expocode

    pass


def create_end_line():

    return 'END_DATA'


def get_data_units():

    # In column names, units are in []

    # Pressure:CTD [dbar],
    # Temperature:CTD [deg_C_(ITS90)],
    # Salinity:CTD [PSS-78],,
    # Transmissivity:CTD [*/m],
    # Oxygen:Dissolved:CTD:Mass [µmol/kg],
    # Fluorescence:CTD:Seapoint [mg/m^3],

    data_units = {}
    data_units['CTDPRS'] = 'DBAR'
    data_units['CTDTMP'] = 'ITS-90'
    data_units['CTDSAL'] = 'PSS-78'
    data_units['CTDOXY'] = 'UMOL/KG'
    data_units['CTDXMISS'] = '*/M'
    data_units['CTDFLUOR'] = 'MG/M^3'

    data_units['CTDPRS_FLAG_W'] = ''
    data_units['CTDTMP_FLAG_W'] = ''
    data_units['CTDSAL_FLAG_W'] = ''
    data_units['CTDOXY_FLAG_W'] = ''
    data_units['CTDXMISS_FLAG_W'] = ''
    data_units['CTDFLUOR_FLAG_W'] = ''

    return data_units




# def get_unique_stnbr_castno(df):

#     # Get unique values of STNBR_CASTNO column
#     stnbr_castno_df = df['STNBR_CASTNO']
#     unique_stnbr_castno_df = stnbr_castno_df.drop_duplicates()

#     return unique_stnbr_castno_df


def get_unique_station_castno(df):

    # Get unique values of STATION_CASTNO column
    station_castno_df = df['STATION_CASTNO']
    unique_station_castno_df = station_castno_df.drop_duplicates()

    return unique_station_castno_df


def get_data_row_sets(df, unique_station_castno_df):

    data_row_sets = []

    # Get list of unique dataframes
    unique_station_castno_list = unique_station_castno_df.tolist()

    # Convert unique_station_castno_df to a list
    # For each number in list, get subset of df
    # and append to data_row_sets

    for station_castno in unique_station_castno_list:

        df_subset = df.loc[df['STATION_CASTNO'] == station_castno]

        # Keep only data columns
        df_subset = get_data_columns(df_subset)

        data_row_sets.append(df_subset)

    return data_row_sets


def create_header_lines(df, data_units):

    data_headers = []

    # Get columns from df
    # Make string from columns in order
    # Make string from data_units in column order



    return data_headers




# def get_data_row_sets(df, unique_stnbr_castno_df):

#     data_row_sets = []

#     # Get list of unique dataframes
#     # dfList = df['one'].tolist()
#     unique_stnbr_castno_list = unique_stnbr_castno_df.tolist()

#     # Convert unique_stnbr_castno_df to a list
#     # For each number in list, get subset of df
#     # and append to data_row_sets

#     for stnbr_castno in unique_stnbr_castno_list:

#         df_subset = df.loc[df['STNBR_CASTNO'] == stnbr_castno]

#         data_row_sets.append(df_subset)

#     return data_row_sets






# def get_station_26(df):

#     try:
#         df = df[df['station'] == 'P26']
#     except:
#         df = df[df['LOC:STATION'] == 'P26']
    
#     df.reset_index(drop=True,inplace=True)
    
#     return df


# def get_station_1(df):

#     try:
#         df = df[df['station'] == 'P1']
#     except:
#         df = df[df['LOC:STATION'] == 'P1']

#     df.reset_index(drop=True,inplace=True)
    
#     return df


# def get_station(df,station):

#     stn = 'P' + station
    
#     try:
#         df = df[df['station'] == stn]
#     except:
#         df = df[df['LOC:STATION'] == stn]
    
#     df.reset_index(drop=True,inplace=True)
    
#     return df



def main():

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
    #     ('2009', '09'), ('2009', '10'), #2009 03 is omitted due to formatting error
    #     ('2008', '01'), ('2008', '26'), ('2008', '27'),
    #     ('2007', '01'), ('2007', '13'), ('2007', '15'),
    # ]

    # expocode_list = [
    #     18LU20180218,
    #     18DD20170205,
    #     18DD20170604,
    #     18DD20170815,
    #     18DD20160208,
    #     18DD20160605,
    #     18DD20160816,
    #     18DD20150210,
    #     18DD20150607,
    #     18DD20150818,
    #     18DD20140210,
    #     18DD20140608,
    #     18DD20140819,
    #     18DD20130205,
    #     18DD20130607,
    #     18DD20130820,
    #     18DD20120206,
    #     18DD20120522,
    #     18DD20120814,
    #     18DD20110208,
    #     18DD20110603,
    #     18DD20110816,
    #     18DD20100202,
    #     18DD20100605,
    #     18DD20100803,
    #     18DD20090127,
    #     18DD20090606,
    #     18DD20090818,
    #     18DD20080129,
    #     18DD20080528,
    #     18DD20070207,
    #     18DD20070530,
    #     18DD20070814
    # ]



    # Run for one cruise
    cruise_list =[ 
        ('2017', '01')
    ]    

    expocode_list = [
        '18DD20170205'
    ]

    # Get index of loop to find corresponding expocode
    #for cruise in cruise_list:
    for index, cruise in enumerate(cruise_list):

        # Get URL of cruise information
        # url: https://www.waterproperties.ca/linep/2017-01/donneesctddata/2017-01-ctd-cruise.csv
        url = build_url(cruise[0],cruise[1])

        # Get dataframe holding all Station P data lines
        df = load_p_line_cruise(url)

        # Rename all data columns 
        df = rename_pline_columns(df)

        # Insert flag columns
        df = insert_flag_colums(df)

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
        unique_station_castno_df = get_unique_station_castno(df)


        # Get data row sets from dataframe for unique station and castno
        data_row_sets = get_data_row_sets(df, unique_station_castno_df)

        print(data_row_sets[1])

        # Get data units from all columns
        # At the moment, it is hard coded in
        #data_units = get_data_units()

       
        # Create column and data units lines
        #data_headers = create_header_lines(df, data_units)



        # Get metadata columns from dataframe for data row sets

        # Create metadata dictionary for each row set
        # create_metadata_dict(metadata_df)



        # Write data sets to file

        # Loop over unique row sets (STATION and CASTNO)

            # Get output file top line
            # top_line = create_top_line(expocode)

            # Get output file end line
            # end_line = create_end_line()        


            # Create filename

            # format stnbr and castno for filename

            # ctd_file = './line_p/expocode/' + expocode + '_' + stnbr_formatted + '_' + castno_formatted + '_ct1.csv'

            # Write top line
            # Write metadata

            # Write out header file which is the following:
            # Write column names
            # Write data units
            

            # Write data
            # Only write data with columns in column_names

            # Write end line





        # As an example, extract out P1 and P26 to dataframes
        #df_1 = get_station_1(df)
        #df_26 = get_station_26(df)

        # Save dataframe values to file in directory line_p
        #df1_file = './line_p/cruise-' +cruise[0] + '-' + cruise[1] +'-station-1.csv'
        #df26_file = './line_p/cruise-' +cruise[0] + '-' + cruise[1] +'-station-26.csv'
        
        #df_1.to_csv(df1_file,index=False)
        #df_26.to_csv(df26_file,index=False)

   



if __name__ == '__main__':

  main()
