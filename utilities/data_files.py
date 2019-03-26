import os
import datetime
import csv

import utilities.headers as headers
import utilities.data_columns as data_columns


def create_start_end_lines():

    # Create starting and ending lines for an exchange format

    now = datetime.datetime.now()

    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')

    start_line = "CTD,{}{}{}CCHSIOLMM".format(year,month,day)

    end_line = 'END_DATA'

    return start_line, end_line


def get_ctd_filename(data_set):

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


def fill_values_in_file(ctd_filename):

    # Replace blanks with '-999' and change fill of -99.0 to -999
    new_lines = []

    with open(ctd_filename, 'r', encoding='utf-8') as f:
        csv_reader = csv.reader(f)

        for line in csv_reader:

            new_line = []
            
            for entry in line:
                if not entry or entry == '-99.0':
                    entry = '-999'

                new_line.append(entry)

            new_line = ','.join(new_line)
            new_lines.append(new_line)


    # write new_lines to file
    with open(ctd_filename, 'w', encoding='utf-8') as f:
        for line in new_lines:
            f.write("{}\n".format(line))    


def write_data_to_file(station_castno_df_sets, comment_header, meta_params, data_params):

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
    start_line, end_line = create_start_end_lines()

    # Create column and data units lines
    column_headers = headers.create_column_headers(data_params)

    # Loop over unique row sets (STATION and CASTNO) and save each to a file      
    for data_set in station_castno_df_sets:

        # Get filename
        ctd_filename = get_ctd_filename(data_set)

        # Create metadata headers
        # Since all the same, use first data set
        metadata_header = headers.create_metadata_header(data_set)        

        # Get data columns
        data_columns_df = data_columns.get_data_columns(data_set, data_params)

        # Write dataframe to csv file so data formatted properly by pandas
        # Don't write index column to file
        data_columns_df.to_csv(ctd_filename, sep=',', index=False, header=False, encoding='utf-8')

        # Replace NaN with '-999' and change fill of -99.0 to -999
        fill_values_in_file(ctd_filename)


        # Read in data from file just created from dataframe
        with open(ctd_filename, 'r', encoding='utf-8') as original: 
            data = original.read()

        # Create concatenaated string to prepend
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



