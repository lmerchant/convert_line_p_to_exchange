"""

Create ctd exchange file for Canadian cruises from 2007-01 to 2018-026

Have checked parameter names for this grouping of cruises and can't guarantee things 
will be the same for other years.  So if run program for new cruises, check 
parameter names and date format.  Currently there are two date formats. One is dd-mm-yy
and the other is dd/mm/yyyy. 


URL of cruise files that contain all cruise lines and their data,
url: https://www.waterproperties.ca/linep/2017-01/donneesctddata/2017-01-ctd-cruise.csv
This file contains more than just Line P data. In this program, only the Line P data is
converted to exchange format.




Read in all data lines into a data frame, then filter P line and sort.
Then group rows by P line and it's sequential event number to find castno

For cruise list, need to have corresponding expocodes in corresponding order



---------------------------------

Instructions:
-------------



Testing:
--------
To test the script, set TESTING to True in process_raw_data.py
Set name of testing file to use.



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

        # Set TESTING variable in raw_data to True of False for
        # setting which data to process
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
        # Leave as is for now

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
