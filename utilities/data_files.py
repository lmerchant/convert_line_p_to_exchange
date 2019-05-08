import os
import datetime
import csv
from urllib.request import urlopen

import utilities.headers as headers
import utilities.data_columns as data_columns
import utilities.process_raw_data as process_raw_data



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
 
    # pad stnbr and castno so have 5 digit chars and prepend 0
    str_stnbr = str_stnbr.zfill(5)
    str_castno = str_castno.zfill(5)

    ctd_file = expocode + '_' + str_stnbr + '_' + str_castno + '_ct1.csv'

    ctd_filename = directory + ctd_file

    return ctd_filename


def get_line_p_cruise_id(expocode, cruise_list):

    # Find tuple for expocode
    found_cruise = [cruise for cruise in cruise_list if cruise[2] == expocode]

    line_p_year = found_cruise[0][0]
    line_p_id = found_cruise[0][1]

    return line_p_year, line_p_id


def get_individual_raw_file(expocode, data_set):

    # Get rows with unique event numbers
    df_unique_events = data_set['EVENT'].unique()
    unique_events = df_unique_events.tolist()


    cruise_list = process_raw_data.get_cruise_list()


    line_p_year, line_p_id = get_line_p_cruise_id(expocode, cruise_list)

    for event in unique_events:
        # Pad event number with leading 0
        event = str(event).zfill(4)

        filename = line_p_year + '-' + line_p_id + '-' + event + '.ctd'

        url = 'https://www.waterproperties.ca/linep/' + line_p_year + '-' + line_p_id + '/donneesctddata/' + filename

        return url


def get_individual_raw_file_header(url):

    txt = urlopen(url).read()
    decode_txt = txt.decode('windows-1252')
    file_text = decode_txt.split('\r\n')


    # Get lines up to line *END OF HEADER
    comment_header = []

    count = 0
    for line in file_text:

        #if '*END OF HEADER' not in line:
        if 'For details on the processing' not in line:
            # then line is a comment header
            # prepend a # sign
            line_comment = '#' + line
            comment_header.append(line_comment)
            count = count+1
        else:
            # Get one more line
            # Get For details on the processing line
            line_comment = '#' + line
            comment_header.append(line_comment)         
            break

    return comment_header


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


        # Get filename of individal raw file to get header
        url = get_individual_raw_file(expocode, data_set)

        try:
            raw_individual_comment_header = get_individual_raw_file_header(url)
        except:
            if url == 'https://www.waterproperties.ca/linep/2009-09/donneesctddata/2009-09-0014.ctd':
                # Error in concatenated file using event 14 for P4 when web page
                # table says it should be event 15. So fixed for this special case
                # https://www.waterproperties.ca/linep/2009-09/index.php
                url = 'https://www.waterproperties.ca/linep/2009-09/donneesctddata/2009-09-0015.ctd'

            elif url == 'https://www.waterproperties.ca/linep/2009-09/donneesctddata/2009-09-0051.ctd':
                # Error in concatenated file using event 51 for P19 when web page
                # table says it should be event 52. So fixed for this special case
                # https://www.waterproperties.ca/linep/2009-09/index.php
                url = 'https://www.waterproperties.ca/linep/2009-09/donneesctddata/2009-09-0052.ctd'
            
            else:
                # Can't open raw individual comment header so use concatenated files header instead
                raw_individual_comment_header = comment_header
                print("Can't find individual CTD file so using concatenated for file: " + ctd_filename)
        

        # Get data columns
        data_columns_df = data_columns.get_data_columns(data_set, data_params)


        # Change flag from 2 to 9 if value in column to left of flag column is -999 or -999.0
        # Flag = 9 represents data not sampled
        data_columns.update_flag_for_fill_999(data_columns_df, data_params)

        # Change flag from 2 to 5 if value in column to left of flag column is -99 or -99.0
        # Flag = 5 represents data not reported
        data_columns.update_flag_for_fill_99(data_columns_df, data_params)  


        # Convert data columns to string 
        # replace -999.0 with -999 which is exchange fill value (has to be integer)
        # replace -99.0 and -99 with -999
        # replace -99 with -999 
        data_columns_df = data_columns_df.applymap(str)

        data_columns_df.replace('-999.0', '-999', inplace=True)
        data_columns_df.replace('-99.0', '-999', inplace=True)
        data_columns_df.replace('-99', '-999', inplace=True)


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


        for line in raw_individual_comment_header:
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

