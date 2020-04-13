"""

This program takes raw data from cruises listed at 
https://www.waterproperties.ca/linep/cruises.php and 
processes the data into the Exchange format.
What I call raw data is the original data as is before any
transformation to Exchange format.

The code takes a concatenated file composed of individual ctd files, identified by event numbers and station id, and converts each station to Exchange format with cast id, coming from incrementing event number, and station number.

Have mapped parameter names in individual files to Exchange parameters
and converted date format for this grouping to Exchange format for
cruises 2007-2018. Due to parameter names possibly changing and any 
date formats changing, can't guarantee things will be the same 
for other years.  

Read in all data lines into a data frame, then filter on P line and 
sort on station. Then group rows by station and its increasing 
event number to map to incremented castno.

For cruise list, need to map corresponding cruise ids to expocodes.

Files output to a data folder defined in config_create_line_p.py. 
It assumes this folder exists.


"""


import utilities.process_raw_data as raw_data
import utilities.headers as headers
import utilities.parameters as params
import utilities.data_columns as data_columns
import utilities.data_files as data_files


def main():

    # Get cruise list which maps Line P cruise identifiers to expocodes
    cruise_list = raw_data.get_cruise_list()

    for cruise in cruise_list:

        # Get URL of cruise file which is concatenated file of all stations
        # e.g. url: https://www.waterproperties.ca/linep/2017-01/donneesctddata/2017-01-ctd-cruise.csv
        url = raw_data.build_url(cruise[0],cruise[1])


        # Get comment header of the concatenated file. Originally used this
        # in output Exchange file but now use individual header from data file
        # of each station. Get header in case want to use it in future.
        comment_header, column_names, data = raw_data.get_headers_and_data(url)

        
        # Insert data into a dataframe with column names from file
        # and only keep stations where station name starts with P.
        # Replace any NaN or empty strings with Exchange fill of -999
        df = raw_data.insert_into_dataframe(column_names, data)


        # Get metadata and data parameters mapping
        # of data column names to WHP names
        meta_params = params.get_meta_params()
        data_params = params.get_data_params(df)


        # Rename all metadata and data parameters to WHP format
        df = data_columns.rename_pline_columns(df, meta_params, data_params)


        # Insert flag columns for each data column since none exist.
        # For now, all flags = 2. Will modify flag later according to
        # fill value of -99 (not reported, flag=5) or 
        # empty value (not sampled, flag=9).
        # Also add flag columns to data_params which param mapping of
        # original column names to WHP names
        df, data_params = data_columns.insert_flag_colums(df, data_params)


        # Reformat date column to Exchange format
        df = data_columns.reformat_date_column(df)

        # Reformat time column to Exchange format
        df = data_columns.reformat_time_column(df)

        # Get expocode from cruise
        expocode = cruise[2]

        # Insert expocode column so can reference from df later
        df = data_columns.insert_expocode_column(df, expocode)

        # Insert castno column
        # Creating castno from sorted list of unique event numbers
        # for each station
        df = data_columns.insert_castno_column(df)

        # Create and insert station_castno column to sort on later
        # and break up dataframe into unique station_castno sets to
        # then later save to a file
        df = data_columns.insert_station_castno_column(df)

        # Get unique values of station_castno column
        unique_station_castno_sets = data_columns.get_unique_station_castno_sets(df)

        # Get dataframe sets for unique station and castno.
        # Breaks up station dataframe into df subsets of unique station/castno
        # that will be saved to its own file
        station_castno_df_sets = data_columns.get_station_castno_df_sets(df, unique_station_castno_sets)


        # Loop through sets and write each set to files and save
        # in folder chosen in config file.
        # Write header, metadata, and data and use WHP column names (data_params).
        # When looping through sets, change flags from 2 to 5 for 
        # data fills of -99 and change flags from 2 to 9 for fills of -999.
        # Then change data fills of -99 to -999 which is the Exchange
        # fill value

        # The input comment_header is for the concatenated file.
        # If a comment header for an individual file for an event number 
        # does not exist, will write the header from the concatenated 
        # file will be used instead. This occurs when there is an
        # event number in the concatenated file that does not match the
        # event number on the url listing a table of stations and their
        # corresponding event number.
        data_files.write_data_to_file(station_castno_df_sets, comment_header, data_params)

        print("Completed " + expocode)

    


if __name__ == '__main__':

    main()
