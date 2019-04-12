import os
import datetime
import csv

import utilities.headers as headers
import utilities.data_columns as data_columns


# Top folder to save data folders in
TOP_DATA_FOLDER = './exchange_line_p_data'


def create_start_end_lines():

    # Create starting and ending lines for exchange format

    now = datetime.datetime.now()

    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')

    start_line = "CTD,{}{}{}CCHSIOLM".format(year,month,day)

    end_line = 'END_DATA'

    return start_line, end_line


def get_ctd_filename(directory, data_set):

    # Create filename to store data to

    # From first row, get expocode, stnbr, and castno
    # needed to create filename
    first_row = data_set.iloc[0]

    expocode = first_row['EXPOCODE']
    stnbr = first_row['STATION']
    castno = first_row['CASTNO']

    str_stnbr = str(stnbr)
    str_castno = str(castno)

    # if stnbr has a slash in name, replace with a dash because
    # can't use / as part of filename.
    if '/' in str_stnbr:
        str_stnbr = str_stnbr.replace('/', '-')
 

    ctd_file = expocode + '_' + str_stnbr + '_' + str_castno + '_ct1.csv'
    ctd_filename = directory + ctd_file

    return ctd_filename
   


def write_data_to_file(station_castno_df_sets, comment_header, meta_params, data_params):

    # Write data sets to files

    # Get expocode to make directory for files
    first_row = station_castno_df_sets[0].iloc[0]
    expocode = first_row['EXPOCODE']

    # Make sub directory in parent folder
    directory = TOP_DATA_FOLDER + '/' + expocode + '_ct1/'

    if not os.path.exists(directory):
        os.makedirs(directory)


    # Get file start and end lines
    #start_line, end_line = create_start_end_lines(station_castno_df_sets[0])
    start_line, end_line = create_start_end_lines()

    # Create column and data units lines
    column_headers = headers.create_column_headers(data_params)

    # Loop over row sets (STATION and CASTNO) and save each to a file      
    for data_set in station_castno_df_sets:

        # Get filename using info from data_set
        ctd_filename = get_ctd_filename(directory, data_set)

        # Create metadata headers using info from data_set
        metadata_header = headers.create_metadata_header(data_set)        

        # Get data columns
        data_columns_df = data_columns.get_data_columns(data_set, data_params)

        # Change flag from 2 to 9 if value in column to left of flag column is -999.0
        data_columns.update_flag_for_fill_999(data_columns_df, data_params)

        # Change flag from 2 to 5 if value in column to left of flag column is -99.0
        data_columns.update_flag_for_fill_99(data_columns_df, data_params)        

        # Convert data columns to string and replace -999.0 with -999
        data_columns_df = data_columns_df.applymap(str)
        data_columns_df.replace('-999.0', '-999', inplace=True)

        # Convert data columns to string and replace -99.0 with -999
        data_columns_df = data_columns_df.applymap(str)
        data_columns_df.replace('-99.0', '-999', inplace=True)


        # Write dataframe to csv file so data formatted properly by pandas.
        # Don't write index column to file. 
        # Will add header below.
        data_columns_df.to_csv(ctd_filename, sep=',', index=False, header=False, encoding='utf-8')


        # Read in data from file just created from dataframe to
        # prepend and append lines
        with open(ctd_filename, 'r', encoding='utf-8') as original: 
            data = original.read()

        # Create concatenated string to prepend
        # Contains exchange start line, comments, and parameter column names
        prepend_string = ''
        comment_header_string = ''
        metadata_header_string = ''
        column_header_string = ''

        start_line_str = start_line + '\n'

        for line in comment_header:
            comment_header_string = comment_header_string + line + '\n' 

        for line in metadata_header:
            metadata_header_string = metadata_header_string + line + '\n'   

        for line in column_headers:
            column_header_string = column_header_string + line + '\n' 

        prepend_string = start_line_str + comment_header_string + metadata_header_string + column_header_string


        # Rewrite over ctd file with prepended text to data csv section
        # Prepend ctd file
        with open(ctd_filename, 'w', encoding='utf-8') as modified: 
            modified.write(prepend_string + data)

        # Append ctd file with end line
        with open(ctd_filename, 'a', encoding='utf-8') as f:
            f.write("{}\n".format(end_line))



