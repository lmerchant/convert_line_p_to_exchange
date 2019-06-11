import utilities.parameters as params


def create_column_headers(data_params):

    """
    Create column headers which are two lines
    Column names in one line followed by corresponding units in next line
    """

    column_headers = []

    # Get data units from all columns
    data_units_dict = params.get_data_units(data_params)

    column_name_row = []
    column_units_row = []

    for key, value in data_units_dict.items():
        column_name_row.append(key)
        column_units_row.append(value)

    # Join rows with a comma
    name_row = ','.join(column_name_row)
    units_row = ','.join(column_units_row)

    column_headers.append(name_row)
    column_headers.append(units_row)

    return column_headers


def create_metadata_header(data_set):

    """
    Create metadata header using values in first row of dataframe
    """

    metadata_header = []

    # Since metadata same in all rows, pick first row to get values from
    first_row = data_set.iloc[0]


    # CASTNO is an integer so cast to string

    metadata_header.append('EXPOCODE = ' + first_row['EXPOCODE'])
    metadata_header.append('STNBR = ' + first_row['STATION'])
    metadata_header.append('CASTNO = ' + str(first_row['CASTNO']))
    metadata_header.append('DATE = ' + first_row['DATE'])
    metadata_header.append('TIME = ' + first_row['TIME'])
    metadata_header.append('LATITUDE = ' + first_row['LATITUDE'])
    metadata_header.append('LONGITUDE = ' + first_row['LONGITUDE'])


    # NUMBER_HEADERS counts all metadata values plus itself
    number_headers = len(metadata_header) + 1
    number_headers_line = 'NUMBER_HEADERS = ' + str(number_headers)

    # Insert NUMBER_HEADERS above metadata values
    metadata_header.insert(0, number_headers_line)

    return metadata_header
