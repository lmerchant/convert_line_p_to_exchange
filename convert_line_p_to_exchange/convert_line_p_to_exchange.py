"""

Create ctd exchange files for Canadian Line P cruises for 2007 through 2018

Have checked parameter names for this grouping of cruises and can't guarantee things 
will be the same for other years.  So if run program for new cruises, check 
parameter names and date format.  Currently there are two date formats. One is dd-mm-yy
and the other is dd/mm/yyyy. 

URL of cruise files that contain all cruise lines and their data
As an example for 2017-01 cruise,
url: https://www.waterproperties.ca/linep/2017-01/donneesctddata/2017-01-ctd-cruise.csv
This file contains more than just Line P data. In this program, only the Line P data is
converted to exchange format. This is also a concatenated file of all individual
station files.

Read in all data lines into a data frame, then filter on P line and sort on station.
Then group rows by station and its sequential event number to find castno

For cruise list, need to map corresponding expocodes to cruise id

Files output to a top level folder defined in config_create_line_p.py. 
It assumes this folder exists.


---------------------------------

To run:
-------------

Run as python3 

In config file, set folder to save all the sets to
Currently TOP_DATA_FOLDER='./exchange_line_p_data'

and whether Testing or not



Testing:
--------
To test the script, set TESTING to True in config_create_line_p.py
and uncomment name of testing file to use in in utilities/process_raw_data.py


"""


import utilities.process_raw_data as raw_data
import utilities.headers as headers
import utilities.parameters as params
import utilities.data_columns as data_columns
import utilities.data_files as data_files


def main():

    # Get cruise list which maps Line P file identifiers to expocodes
    cruise_list = raw_data.get_cruise_list()

    for cruise in cruise_list:

        # Get URL of cruise file which is concatenated file of all stations
        # e.g. url: https://www.waterproperties.ca/linep/2017-01/donneesctddata/2017-01-ctd-cruise.csv
        url = raw_data.build_url(cruise[0],cruise[1])


        # Get comment header of the concatenated file. Originally used this
        # in output Exchange file but now use individual header from data file
        # of each station. Get header in case want to use it in future.
        comment_header, column_names, data = raw_data.get_headers_and_data(url)

        
        # Insert data into a dataframe with column names
        # and only keep stations where station name starts with P
        df = raw_data.insert_into_dataframe(column_names, data)


        # Get meta and data parameters mapping
        # of raw data column name to WHP name
        meta_params = params.get_meta_params()
        data_params = params.get_data_params(df)


        # Rename all meta and data parameters to WHP format
        df = data_columns.rename_pline_columns(df, meta_params, data_params)


        # Insert flag columns for each data column since none exist.
        # For now, all flags = 2. Will modify flag later according to
        # fill value of -99 or -999.
        # Also add flag columns to data_params
        df, data_params = data_columns.insert_flag_colums(df, data_params)


        # Reformat date column to Exchange format
        df = data_columns.reformat_date_column(df)

        # Reformat time column to Exchange format
        df = data_columns.reformat_time_column(df)

        # Get expocode from cruise list
        expocode = cruise[2]

        # Insert expocode column
        # Used to create filename later to write output to
        df = data_columns.insert_expocode_column(df, expocode)

        # Insert castno column
        # Creating castno from sorted list of unique event numbers
        # for each station
        df = data_columns.insert_castno_column(df)

        # Create and insert station_castno column to mark CTD sets
        df = data_columns.insert_station_castno_column(df)


        # Get unique station_castno sets so can have list of station_castno data sets to save
        unique_station_castno_sets = data_columns.get_unique_station_castno_sets(df)

        # Get dataframe sets for unique station and castno.
        # Breaks up station dataframe into subsets of unique station/castno
        # that will be saved to its own file
        station_castno_df_sets = data_columns.get_station_castno_df_sets(df, unique_station_castno_sets)


        # Loop through sets and write each set to files in a folder given in config file.
        # Will write data, metadata and use column names (data_params)
        # The comment header is for the concatenated file and if exists a 
        # comment header for an individual file for an event number, will write
        # that header to the file instead.
        data_files.write_data_to_file(station_castno_df_sets, comment_header, data_params)

        print("Completed " + expocode)

    


if __name__ == '__main__':

    main()