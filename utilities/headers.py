import utilities.parameters as params


def create_column_headers(data_params):

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

    metadata_header = []

    first_row = data_set.iloc[0]

    metadata_header.append('EXPOCODE = ' + first_row['EXPOCODE'])
    metadata_header.append('STNBR = ' + first_row['STATION'])
    metadata_header.append('CASTNO = ' + str(first_row['CASTNO']))
    metadata_header.append('DATE = ' + first_row['DATE'])
    metadata_header.append('TIME = ' + first_row['TIME'])
    metadata_header.append('LATITUDE = ' + str(first_row['LATITUDE']))
    metadata_header.append('LONGITUDE = ' + str(first_row['LONGITUDE']))

    number_headers = len(metadata_header) + 1
    number_headers_line = 'NUMBER_HEADERS = ' + str(number_headers)

    metadata_header.insert(0, number_headers_line)

    return metadata_header
