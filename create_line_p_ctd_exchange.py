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
To test the script, comment/uncomment the following lines in get_headers_and_data 

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


import utilities.process_raw_data as raw_data
import utilities.headers as headers
import utilities.parameters as params
import utilities.data_columns as data_columns
import utilities.data_files as data_files


def main():

    # Get cruise list mapping Line P web files to expocodes
    cruise_list = raw_data.get_cruise_list()

    for cruise in cruise_list:

        # Get URL of cruise information
        # url: https://www.waterproperties.ca/linep/2017-01/donneesctddata/2017-01-ctd-cruise.csv
        url = raw_data.build_url(cruise[0],cruise[1])

        # Get headers and data from all Station P data lines
        comment_header, parameter_header, data = raw_data.get_headers_and_data(url)
        
        # Insert data into a dataframe with column names from parameter_header
        # Only keep Line P stations where station name starts with P
        df = raw_data.insert_into_dataframe(parameter_header, data)

        # Get meta and data parameters with
        # mapping of pline names to WHP names
        meta_params = params.get_meta_params()
        data_params = params.get_data_params(df)

        # Rename all meta and data parameters to WHP format
        df = data_columns.rename_pline_columns(df, meta_params, data_params)

        # Insert flag columns
        df, data_params = data_columns.insert_flag_colums(df, data_params)

        # Reformat date column to Exchange format
        df = data_columns.reformat_date_column(df)

        # Get expocode from cruise list
        expocode = cruise[2]

        # TODO.  Do I need to insert expocode column?
        # Only using to extract expocode when write data to file.
        # Could just pass expcode in as parameter to write_data_to_file

        # Insert expocode column
        df = data_columns.insert_expocode_column(df, expocode)


        # Insert castno column
        # Creating castno from sorted list of unique event numbers
        # for each station
        df = data_columns.insert_castno_column(df)

        # Create and insert station_castno column to mark CTD sets
        df = data_columns.insert_station_castno_column(df)
    
        # Get unique station_castno sets so can have list of station_castno data sets will save
        unique_station_castno_sets = data_columns.get_unique_station_castno_sets(df)


        # Get data sets from dataframe for unique station and castno
        # Breaks up station dataframe into subsets of unique station/castno
        # that will be saved to its own file
        station_castno_df_sets = data_columns.get_station_castno_df_sets(df, unique_station_castno_sets)

        # loop through sets and write each set to a file
        data_files.write_data_to_file(station_castno_df_sets, comment_header, meta_params, data_params)

        print("Completed " + expocode)

        break



if __name__ == '__main__':

    main()
