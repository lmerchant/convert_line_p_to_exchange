import shutil
import pandas as pd
from urllib.request import urlopen

from config_create_line_p import Config


def get_cruise_list():

    # list mapping Line P web files to expocodes

    # Canadian Line P cruise data of format
    # year-identifier. Check website for values
    # https://www.waterproperties.ca/linep/cruises.php

    # if cruise is 2009-03 with expocode 18DD20090127, it is 
    # a special case. Will be using 2009-03 cruise file in testing folder 
    # where the original file from the url has been corrected
    # to delete the duplicate station column and shifting the column
    # headers to the left. The original cruise file is then saved
    # with the name 18DD20090127_2009-03-ctd-cruise.csv into the 
    # testing folder 
    # filename = "./test/data/18DD20090127_2009-03-ctd-cruise.csv"


    # line p year, cruise identifier, and corresponding expocode
    cruise_list = [
        ('2018', '040', '18DD20180911'),
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


    if Config.TESTING:
        # use dummy value for cruise list and expocode = 'TESTING'
        cruise_list = [('year', 'cruise_id', 'TESTING')]

    return cruise_list        


def build_url(year, cruise_id):
    url = 'https://www.waterproperties.ca/linep/' + year + '-' + cruise_id + '/donneesctddata/' + year + '-' + cruise_id + '-ctd-cruise.csv'

    return url


def get_raw_csv(url):

    # Read in csv file and split text on returns to get a list of lines

    # Ran chardetect in terminal on cruise csv file downloaded from web page
    # and returns encoding of windows-1252

    if Config.TESTING:

        # Delete testing data output folder before writing more
        try:
            testing_output_folder = Config.TOP_DATA_FOLDER + '/TESTING_ct1'
            shutil.rmtree(testing_output_folder)
            print("Deleting previous testing folder")
        except:
            print("Testing folder will be created")

        # Create test files in windows-1252 with windows line endings
        
        #test_filename = "./test/data/data_to_test_castno.csv"
        #test_filename = "./test/data/data_to_test_castno_one_pline.csv"
        #test_filename = "./test/data/data_to_test_fill_999.csv"
        #test_filename = "./test/data/data_to_test_fill_999_2.csv"
        test_filename = "./test/data/data_to_test_fill_999_3.csv"
        #test_filename = "./test/data/data_to_test_date_format_w_dash.csv"
        #test_filename = "./test/data/data_to_test_date_format_w_slash.csv"
        #test_filename = "./test/data/data_to_test_column_names1.csv"
        #test_filename = "./test/data/data_to_test_column_names2.csv"
        #test_filename = "./test/data/data_to_test_column_names3.csv"
        #test_filename = "./test/data/data_to_test_column_names4.csv"
        #test_filename = "./test/data/data_to_test_column_names5.csv"
        #test_filename = "./test/data/data_to_test_flag_values.csv"
        #test_filename = "./test/data/data_to_test_formatting.csv"
        
        #test_filename = "./test/data/18DD20170205_2017-01-ctd-cruise.csv"
        #test_filename = "./test/data/18DD20090127_2009-03-ctd-cruise.csv"

        with open(test_filename, 'r', encoding='windows-1252') as f:
            decode_text = f.read()

        raw_csv = decode_text.split('\n')

    elif url == 'https://www.waterproperties.ca/linep/2009-03/donneesctddata/2009-03-ctd-cruise.csv':

        # Open 2009-03 cruise file in testing folder where file has been corrected
        # to delete the duplicate station column and shifting the column
        # headers to the left. The original cruise file is then saved
        # with the name 18DD20090127_2009-03-ctd-cruise.csv into the 
        # testing folder 

        filename = "./test/data/18DD20090127_2009-03-ctd-cruise.csv"

        with open(filename, 'r', encoding='windows-1252') as f:
            decode_text = f.read()

        raw_csv = decode_text.split('\n')

    else:

        txt = urlopen(url).read()
        decode_txt = txt.decode('windows-1252')
        raw_csv = decode_txt.split('\r\n')


    return raw_csv


def get_headers_and_data(url):

    # The url is a csv file
    # This cruise csv file lists all lines from from ctd files and all stations
    # Read in csv file, find header and save.
    # Then read in all data lines and put into a data frame
    # Filter data from for Line P stations, sort and return data frame

    # Sample file column names line, followed by empty line, and then first data line
    # File Name,Zone,FIL:START TIME YYYY/MM/DD, HH:MM,LOC:EVENT_NUMBER,LOC:LATITUDE,LOC:LONGITUDE,LOC:STATION,INS:LOCATION,Pressure:CTD [dbar],Temperature:CTD [deg_C_(ITS90)],Salinity:CTD [PSS-78],Sigma-t:CTD [kg/m^3],Transmissivity:CTD [*/m],Oxygen:Dissolved:CTD:Volume [ml/l],Oxygen:Dissolved:CTD:Mass [µmol/kg],Fluorescence:CTD:Seapoint [mg/m^3],Fluorescence:CTD:Wetlabs [mg/m^3],PAR:CTD [µE/m^2/sec]
    #,,,,,,,,,,,,,,,,,,
    #2017-01-0001.ctd,UTC,06-02-17, 23:58,1,48.65883,-123.49934,SI,Mid-ship,1.1,6.6845,28.1863,22.1205,,,,1.186,,12.6  


    # Read in csv file and split text on returns to get a list of lines
    raw_csv = get_raw_csv(url)


    # Find header and data lines containing values from ctd files.
    # Save original comment header to write to output file later if needed.
    # Want to skipp comment header to read in data to pandas

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

    # Get column names above comma separation line
    column_names = raw_csv[count-2].split(',')

    # clean_csv contains all the data lines
    clean_csv = raw_csv[count:]

    # Put data lines into a list
    data = []
    for row in clean_csv:
        data.append(row.split(','))

    return comment_header, column_names, data


def insert_into_dataframe(column_names, data):

    # Import data lines into a data frame
    
    # column_names is list of all the column names

    # Panda keeps greek characters in column names if there were any
    # Pandas imports all columns as string
    df = pd.DataFrame(data, columns=column_names)


    # drop any rows with empty cell values in Pressure:CTD column
    # Do this because data sets separated by empty rows
    df.dropna(subset=['Pressure:CTD [dbar]'], inplace=True)

    # Get Line P Stations only.  These are Stations starting with P
    # Find rows starting with P in the LOC:STATION column
    df = df[df['LOC:STATION'].str.startswith('P')]


    # Column data are converted to either string or numeric
    # with df.apply

    # Convert any string numeric columns to numeric.
    # String columns kept as dtype Object. 
    # Any empty values will become NaN    

    # Date, Time, and Station are kept as string.
    # Time and date are kept as string because
    # time includes a ':' and date includes a '/' or '-'.
    # Station is imported as string because station name
    # starts with a letter. The rest becomes NaN or float
   
    # use errors='ignore' rather than errors='coerce'
    # because otherwise fails because have coerced string
    # date into a bytes-like object which won't work when
    # trying to use reg expression on a date

    # Don't convert to numeric. Keep as strings so numbers keep precision
    #df = df.apply(pd.to_numeric, errors='ignore')

    # Convert event column and pressure to numeric to sort on
    df = df.astype({'LOC:EVENT_NUMBER': int, 'Pressure:CTD [dbar]': float})


    # Any columns with float values automatically convert to float.
    # because pandas can only have all integers or all floats in
    # a column. For decimal precision of 4 or 5 digits, will need to
    # round values to 4 or 5 places since float read in converted to
    # too many digits.

    # If fill with -999 in a float column, it becomes -999.0  
    # Any -99 fill numbers in float columns are converted to -99.0 

    # Later when the program is saved as a csv exchange file, 
    # any -999.0, -99.0, -99 fill values are set to -999 

    # Replace any NaN cells with -999   
    df.fillna(-999, inplace=True)


    # Replace any empty cells with '-999'
    df = df.replace(r'^\s*$', '-999', regex=True)


    # Sort by station and then pressure and reset index to match new row order
    df.sort_values(by=['LOC:STATION','Pressure:CTD [dbar]'],inplace=True)
    df.reset_index(drop=True,inplace=True)

    return df

