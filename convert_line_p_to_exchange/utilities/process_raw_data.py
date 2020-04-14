import shutil
import os
import pandas as pd
import numpy as np
from urllib.request import urlopen

from config import Config


def get_cruise_list():

    """Get list mapping Line P cruise file ids from website to expocodes"""

    # Canadian Line P cruise data of format
    # year-identifier. Check website for values to use at
    # https://www.waterproperties.ca/linep/cruises.php which 
    # lists the Canadian Line P program cruise links.
    # e.g. 


    # line P year, cruise identifier, and corresponding expocode

    cruise_list = Config.CRUISE_LIST


    if Config.TESTING:
        # use dummy values for cruise with expocode = 'TESTING'
        cruise_list = [('year', 'cruise_id', 'TESTING')]

    return cruise_list        


def build_url(year, cruise_id):
    """Build url of file from year and cruise identifier"""
    url = 'https://www.waterproperties.ca/linep/' + year + '-' + cruise_id + '/donneesctddata/' + year + '-' + cruise_id + '-ctd-cruise.csv'

    return url


def get_raw_csv_from_testing():

    if "TESTING_FILE" in os.environ:
        test_filename = os.environ["TESTING_FILE"]

    else:
        print("No environment var TESTING_FILE set")
        exit(1)


    with open(test_filename, 'r', encoding='windows-1252') as f:
        decode_text = f.read()

    raw_csv = decode_text.split('\n')

    return raw_csv


def get_raw_csv_from_url(url):

    txt = urlopen(url).read()
    decode_txt = txt.decode('windows-1252')
    raw_csv = decode_txt.split('\r\n')

    return raw_csv


def get_raw_csv(url):

    """
    Read in csv data file with encoding windows-1252 and split text 
    on returns to get a list of lines. 

    Ran chardetect in terminal on cruise csv file 
    downloaded from web page
    and it returned an encoding of windows-1252    

    If Config.TESTING = True,
    csv files of encoding windows-1252 are located in testing folder ./tests/data

    Special Case:
    If cruise is 2009-03 with expocode 18DD20090127, it is 
    a special case. Will be using 2009-03 cruise file in 
    adjusted_data folder where the original file from the 
    url has been corrected to delete the duplicate station 
    column and column names adjusted for proper columns.

    """

    if Config.TESTING:

        # Get filename from environmet variable

        raw_csv = get_raw_csv_from_testing()

    elif url == 'https://www.waterproperties.ca/linep/2009-03/donneesctddata/2009-03-ctd-cruise.csv':

        filename = "./adjusted_data/18DD20090127_2009-03-ctd-cruise.csv"

        with open(filename, 'r', encoding='windows-1252') as f:
            decode_text = f.read()

        raw_csv = decode_text.split('\n')

    else:

        raw_csv = get_raw_csv_from_url(url)


    return raw_csv


def get_headers_and_data(url):

    """
    Get comment header, column names, and data lines from CTD csv file.

    Assume column names and data lines have a comma separation line.
    Assume data lines start when '.ctd' is in a line.
    """

    # Sample file column names line, followed by empty line, and then first data line

    # File Name,Zone,FIL:START TIME YYYY/MM/DD, HH:MM,LOC:EVENT_NUMBER,LOC:LATITUDE,LOC:LONGITUDE,LOC:STATION,INS:LOCATION,Pressure:CTD [dbar],Temperature:CTD [deg_C_(ITS90)],Salinity:CTD [PSS-78],Sigma-t:CTD [kg/m^3],Transmissivity:CTD [*/m],Oxygen:Dissolved:CTD:Volume [ml/l],Oxygen:Dissolved:CTD:Mass [µmol/kg],Fluorescence:CTD:Seapoint [mg/m^3],Fluorescence:CTD:Wetlabs [mg/m^3],PAR:CTD [µE/m^2/sec]
    #,,,,,,,,,,,,,,,,,,
    #2017-01-0001.ctd,UTC,06-02-17, 23:58,1,48.65883,-123.49934,SI,Mid-ship,1.1,6.6845,28.1863,22.1205,,,,1.186,,12.6  


    # Read in csv file and split text on returns to get a list of lines
    raw_csv = get_raw_csv(url)


    comment_header = []

    count = 0
    for line in raw_csv:

        if '.ctd' not in line:
            # then line is a comment header
            # prepend with a # sign
            line_comment = '#' + line
            comment_header.append(line_comment)
            count = count + 1
        else:
            # Found ctd data line
            break

    # Get column names above comma separation line
    # count is start of data line so go up two lines for
    # column names
    column_names = raw_csv[count-2].split(',')

    # clean_csv contains all the data lines
    clean_csv = raw_csv[count:]

    # Put data lines into a list. Split on comma to get
    # data value into its own column. Get a list of lists.
    data = []
    for row in clean_csv:
        data.append(row.split(','))

    return comment_header, column_names, data


def drop_empty_rows(df):
    # drop any rows with NaN or None values in Pressure:CTD column
    # Do this because want to drop last row in file that is empty.
    # subset is those columns to inspect for NaNs.

    # First create empty values to NaN
    df['Pressure:CTD [dbar]'].replace('', np.nan, inplace=True)

    # Now drop all with NaN
    df.dropna(subset=['Pressure:CTD [dbar]'], inplace=True)

    return df


def convert_event_pressure_to_numeric(df):
    # Convert event column and pressure to numeric to sort on.
    # Keep rest of data values as strings
    df = df.astype({'LOC:EVENT_NUMBER': int, 'Pressure:CTD [dbar]': float})

    return df


def get_line_p_station_rows(df):
    # Get Line P Stations only.  These are Stations starting with P
    # Find rows starting with P in the LOC:STATION column
    # This will also exclude all blank rows separating events
    df = df[df['LOC:STATION'].str.startswith('P')]

    return df    


def replace_nan_or_empty_cells_with_999(df):
    # Replace any NaN cells with -999 (exchange fill value)  
    df.fillna(-999, inplace=True)

    # Replace any empty cells with '-999' (exchange fill value)
    df = df.replace(r'^\s*$', '-999', regex=True)

    return df


def sort_df(df):
    # Sort by station and then pressure and reset index to match new row order
    df.sort_values(by=['LOC:STATION','Pressure:CTD [dbar]'],inplace=True)
    df.reset_index(drop=True,inplace=True)

    return df    


def reformat_df(df):
    df = drop_empty_rows(df)

    df = get_line_p_station_rows(df)

    # Convert event column and pressure to numeric to sort on.
    # Keep rest of data values as strings
    df = convert_event_pressure_to_numeric(df)

    # Previously converted all columns to numeric but Pandas had
    # trouble with numbers of 4 decimal place precision in that
    # extra precision digits were added. So don't convert to numeric. 
    # df = df.apply(pd.to_numeric, errors='ignore')
    # Keep as strings so numbers keep precision of original.

    df = replace_nan_or_empty_cells_with_999(df)

    # Sort by station and then pressure and reset index to match new row order
    df = sort_df(df)

    return df


def place_data_into_dataframe(column_names, data):
    # Panda keeps greek characters in column names if there were any.
    # Here, Pandas imports all columns as type string.
    # Empty cells interpreted as NaN.

    df = pd.DataFrame(data, columns=column_names)

    return df


def insert_into_dataframe(column_names, data):

    """
    Insert data into pandas dataframe using column_names.
    Data stored as strings except event and pressure which
    are converted to numeric. This is for sorting.
    Remove blank rows. Only keep data for stations starting with P.
    Fill any blanks with a fill of -999.
    """

    df = place_data_into_dataframe(column_names, data)

    df = reformat_df(df)

    return df

