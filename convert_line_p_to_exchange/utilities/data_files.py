import os
import datetime
import csv
from urllib.request import urlopen
import pathlib

import utilities.headers as headers
import utilities.data_columns as data_columns
import utilities.process_raw_data as process_raw_data

from config import Config


def reformat_csv(ctd_filename, column_headers):

    """
    Format data output to be 10 space columns except for flags which are 1 space
    """
    
    # Sample output would be
    #          1,2,    0.4121,2,   33.6184,2,    320.82,1,     0.326,1,     97.69,1,      1.18,1

    # Get column names
    column_headers_str = column_headers[0]
    columns = column_headers_str.split(',')

    new_rows = []

    with open(ctd_filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:  

            new_row = ''
            
            for index, column in enumerate(row):

                if 'FLAG' in columns[index]:
                    new_column = '{:>1}'.format(column)
                    new_row = new_row + ',' + new_column

                else:
                    new_column = '{:>10}'.format(column)
                    new_row = new_row + ',' + new_column

            
            # skip first blank string and comma and add line return
            new_row = new_row[1:] + '\n'
            new_rows.append(new_row)

    with open(ctd_filename, 'w', encoding='utf-8') as f:
        for row in new_rows:
            f.write(row)       


def write_dataframe_to_csv(data_columns_df, ctd_filename, start_line, end_line, header_comments, metadata_header, column_headers):

    """
    Write dataframe to csv file with data formatted
    """

    # Don't write index column to file. 
    # Will add header below.
    data_columns_df.to_csv(ctd_filename, sep=',', index=False, header=False, encoding='utf-8')

    # reformat columns to have equal spacing for params and 1 space for flags
    reformat_csv(ctd_filename, column_headers)

    # Read in data as a string from file just created from dataframe to
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


    for line in header_comments:
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


def create_header(header_comments, citation_text, url):
    
    comments = citation_text

    castno_text = []

    castno_text.append("#")
    castno_text.append("# The ORIGINATOR supplied an event number instead of a CASTNO, so")
    castno_text.append("# CCHDO assigned a CASTNO to each STNNBR by starting the CASTNO at 1 for the ")    
    castno_text.append("# first event number and incrementing by 1 for each sequential event number.")
    castno_text.append('#')

    url_text = []
    url_text.append("# The following header comments are from the file ")
    url_text.append(url)
    url_text.append("# Not all parameters were used in the conversion to Exchange format.")
    url_text.append("#")
    url_text.append("#")

    comments_start_text = []
    comments_start_text.append("# Start of ORIGINATOR file comment header")
    comments_start_text.append("#")

    comments.extend(castno_text)
    comments.extend(url_text)
    comments.extend(comments_start_text)
    comments.extend(header_comments)
    comments.append("#")

    return comments


def get_citation_text():

    filename = 'citation.txt'

    with open(filename, 'r') as f:
        citation_text = f.read().splitlines()

    return citation_text


def get_file_text(url):
    txt = urlopen(url).read()
    decode_txt = txt.decode('windows-1252')
    file_text = decode_txt.split('\r\n')

    return file_text


def get_individual_raw_file_header(url):

    """
    Get header from raw individual file
    """

    file_text = get_file_text(url)


    # Get lines up to line *END OF HEADER
    comment_header = []

    count = 0
    for line in file_text:

        if '*END OF HEADER' not in line:    
            # then line is a comment header
            # prepend a # sign
            line_comment = '#' + line
            comment_header.append(line_comment)
            count = count + 1
        else:
            # Get one more line (The END OF HEADER line)
            line_comment = '#' + line
            comment_header.append(line_comment)         
            break

    return comment_header


def choose_raw_file_header(url, concat_comment_header, ctd_filename):

    """
    From url, extract comment header from raw individual file, but if
    it doesn't exist, use comment_header from concatenated file
    """

    # Some raw individual files exist but not at url concatenated file said it 
    # would be. In that case, use value listed in cruise web page table.

    try:
        header_comments = get_individual_raw_file_header(url)
    
    except:
    
        if url == 'https://www.waterproperties.ca/linep/2009-09/donneesctddata/2009-09-0014.ctd':
            # Error in concatenated file using event 14 for P4 when web page
            # table says it should be event 15. So fixed for this special case
            # https://www.waterproperties.ca/linep/2009-09/index.php
            url = 'https://www.waterproperties.ca/linep/2009-09/donneesctddata/2009-09-0015.ctd'

            header_comments = get_individual_raw_file_header(url)

        elif url == 'https://www.waterproperties.ca/linep/2009-09/donneesctddata/2009-09-0051.ctd':
            # Error in concatenated file using event 51 for P19 when web page
            # table says it should be event 52. So fixed for this special case
            # https://www.waterproperties.ca/linep/2009-09/index.php
            url = 'https://www.waterproperties.ca/linep/2009-09/donneesctddata/2009-09-0052.ctd'

            header_comments = get_individual_raw_file_header(url)
        
        else:
            # Can't open raw individual comment header so use concatenated files header instead
            header_comments = concat_comment_header
            print("Can't find individual CTD file so using concatenated for file: " + ctd_filename)

    return header_comments


def get_line_p_cruise_id(expocode, cruise_list):

    """
    Get cruise year and id given an expocode
    """

    # Find tuple for expocode

    found_cruise = [cruise for cruise in cruise_list if cruise[2] == expocode]

    line_p_year = found_cruise[0][0]
    line_p_id = found_cruise[0][1]

    return line_p_year, line_p_id


def get_individual_raw_file(expocode, data_set):

    """
    Get url of individual raw file
    """

    # Get rows with unique event numbers
    df_unique_events = data_set['EVENT'].unique()
    unique_events = df_unique_events.tolist()

    # Get cruise list mapping of cruise ids to expocodes
    cruise_list = process_raw_data.get_cruise_list()

    line_p_year, line_p_id = get_line_p_cruise_id(expocode, cruise_list)

    event = unique_events[0]

    # Pad event number with leading 0
    event = str(event).zfill(4)

    filename = line_p_year + '-' + line_p_id + '-' + event + '.ctd'

    url = 'https://www.waterproperties.ca/linep/' + line_p_year + '-' + line_p_id + '/donneesctddata/' + filename

    return url


def get_ctd_filename(directory, data_set):

    """
    Create filename to store data to
    """

    # From first row, get expocode, stnbr, and castno
    # needed to create filename
    first_row = data_set.iloc[0]

    expocode = first_row['EXPOCODE']
    stnbr = first_row['STATION']
    castno = first_row['CASTNO']

    str_stnbr = str(stnbr)
    str_castno = str(castno)

    # Remove - in filename from stations like PA-001 so have 5 characters.
    if '-' in str_stnbr:
        str_stnbr = str_stnbr.replace('-', '')

    # if stnbr has a slash in name, replace with a dash because
    # can't use / as part of filename.
    # e.g. is P1/B8 in 2015-09
    if '/' in str_stnbr:
        str_stnbr = str_stnbr.replace('/', '-')

    # pad stnbr and castno so have 5 characters. Prepend with 0
    str_stnbr = str_stnbr.zfill(5)
    str_castno = str_castno.zfill(5)

    ctd_file = expocode + '_' + str_stnbr + '_' + str_castno + '_ct1.csv'

    ctd_filename = directory + ctd_file

    return ctd_filename


def create_start_end_lines():

    """
    Create starting and ending lines for exchange format
    """

    now = datetime.datetime.now()

    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')

    start_line = "CTD,{}{}{}CCHSIOLM".format(year,month,day)

    end_line = 'END_DATA'

    return start_line, end_line


def write_data_to_file(station_castno_df_sets, comment_header, data_params):

    """
    Write data sets to files. 
    """

    # Get expocode to make directory for files
    first_row = station_castno_df_sets[0].iloc[0]
    expocode = first_row['EXPOCODE']

    if Config.TESTING:

        filename = os.environ["TESTING_FILE"]
        file_basename = pathlib.Path(filename).stem
        
        directory = pathlib.Path.cwd() / 'tests' / 'output' / file_basename 

        directory = str(directory) + '/'

    else:

        # Make sub directory in parent folder
        #directory = './exchange_line_p_data/' + expocode + '_ct1/'

        directory = Config.OUTPUT_DATA_FOLDER + '/' + expocode + '_ct1/'


    if not os.path.exists(directory):
        os.makedirs(directory)


    # Get file start and end lines
    start_line, end_line = create_start_end_lines()
    
    citation_text = get_citation_text()

    # Create column names and data units lines
    column_headers = headers.create_column_headers(data_params)

    # Loop over row sets (STATION and CASTNO) and save each to a file      
    for data_set in station_castno_df_sets:

        # Get filename using info from data_set
        ctd_filename = get_ctd_filename(directory, data_set)


        # Create metadata headers using info from data_set.
        metadata_header = headers.create_metadata_header(data_set)  


        # Get url of individal raw file to get header
        url = get_individual_raw_file(expocode, data_set)

        # If raw file found, use that header, else use comment_header from 
        # concatenated file
        header_comments = choose_raw_file_header(url, comment_header, ctd_filename)

        header_comments = create_header(header_comments, citation_text, url)

        # Get data columns from dataframe
        data_columns_df = data_columns.get_data_columns(data_set, data_params)


        # Change flag from 2 to 9 if value in column to left of flag column is -999 or -999.0
        # Flag = 9 represents data not sampled.
        # Used -999.0 before when had converted all columns to numeric values.
        # But then decided to keep all as strings except pressure column.
        # Only numeric column left to search for fill would be pressure column.

        # For string columns with fill of -999
        data_columns_df = data_columns.update_flag_for_fill_999_str(data_columns_df, data_params)


        # Change flag from 2 to 5 if value in column to left of flag column is -99 or -99.0
        # Flag = 5 represents data not reported.
        # Used -99.0 before when had converted all columns to numeric values.
        # Only numeric column left to search for fill would be pressure column.  
 
        # For string columns with fill of -99
        data_columns_df = data_columns.update_flag_for_fill_99_str(data_columns_df, data_params) 


        # Replace all fills to be -999 
        data_columns_df = data_columns.replace_fill_values_in_df(data_columns_df)


        # Finally, write dataframe to file
        write_dataframe_to_csv(data_columns_df, ctd_filename, start_line, end_line, header_comments, metadata_header, column_headers)
      

