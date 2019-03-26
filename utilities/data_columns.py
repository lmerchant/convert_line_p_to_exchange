def rename_pline_columns(df, meta_params, data_params):

    # Rename all pline meta and data parameters

    # meta_params and data_params are mapping dicts
    # between pline column names and WHP column names

    param_dict = {}

    for param in meta_params:
        param_dict[param['longname']] = param['whpname']    

    for param in data_params:
        param_dict[param['longname']] = param['whpname']

    df.rename(columns=param_dict, inplace=True)

    return df


def insert_flag_colums(df, data_params):

    new_params = []

    # Get location of column to insert flag column after
    # Insert flag column with value of 2
    for param in data_params:

        param_name = param['whpname']
        param_loc = df.columns.get_loc(param_name)
        flag_name = '{}_FLAG_W'.format(param_name)

        df.insert(param_loc + 1, flag_name, 2)

        # Insert flag param dict into data_params list
        # Want this so know which columns to extract for saving output and
        # keep order of newly inserted flag column after param column
        param_insert = {}
        param_insert['whpname'] = flag_name
        param_insert['longname'] = flag_name
        param_insert['units'] = ''

        new_params.append(param)
        new_params.append(param_insert)

    return df, new_params


def reformat_date_column(df):

    # Files can have two different date formats
    # dd-mm-yy or dd/mm/yyyyy
    # Check if date contains - or /

    if '-' in df['DATE'][0]:

        # 1) Reformat DATE column from dd-mm-yy to yyyymmdd

        # Check if yy is less than 40. 
        #  if it is, prepend with 20
        #  if it isn't, prepend with 19

        year = df['DATE'][0][-2:]

        pattern = r'(\d\d)-(\d\d)-(\d\d)'

        if int(year) < 40:
            repl = r'20\3\2\1'
        else:
            repl = r'19\3\2\1'

    elif '/' in df['DATE'][0]:

        # 2) Reformat DATE column from dd/mm/yyyy to yyyymmdd
        pattern = r'(\d\d)/(\d\d)/(\d\d\d\d)'
        repl = r'\3\2\1'


    df['DATE'] = df['DATE'].str.replace(pattern, repl)

    return df


def insert_expocode_column(df, expocode):

    # Create EXPOCODE column

    # Insert before STATION column
    STATION_LOC = df.columns.get_loc('STATION')

    df.insert(STATION_LOC - 1, 'EXPOCODE', expocode)

    return df


def populate_castno_one_station(df, station):

    # Incoming df has been sorted on station and event number

    # Want to get station subset and find all the unique event numbers
    # used for that station. Then take these sorted event numbers and 
    # map it to an increasing castno that starts at 1.
    # Once have mapping of event numbers to castno, fill rows of castno column

    # Get dataframe subset for station
    df_subset = df.loc[df['STATION'] == station].copy()
    df_subset.reset_index(drop=True,inplace=True)

    # Get unique events for this station subset
    event_df = df_subset['EVENT'].copy()
    event_df.reset_index(drop=True,inplace=True)

    unique_event_df = event_df.drop_duplicates()
    unique_event_df.reset_index(drop=True,inplace=True)

    # Get list of unique events for station
    unique_event_list = unique_event_df.tolist() 

    # Use index of unique event list to create CASTNO
    # since df was sorted on Event#

    # Events were sorted, so increasing castno corresponding to increasing event #
    for index, event in enumerate(unique_event_list):
        # index of unique event list gives the increasing # indicating a castno
        # Then for each event in df, set CASTNO value
        df.loc[df.EVENT == event, 'CASTNO'] = index + 1  

    return df      


def insert_castno_column(df):

    # Create CASTNO column and fill with dummy value
    # Insert after STATION column    
    STATION_LOC = df.columns.get_loc('STATION')
    df.insert(STATION_LOC + 1, 'CASTNO', 0)

    # Creating castno from sorted list of unique event numbers
    # for each station.  Mapping increasing event number to 
    # increasing castno where castno starts at one and has increment of 1

    # Will be filling the CASTNO column station by station.

    # castno is a mapping of the sequential event numbers of a single station
    # Say the event number series is 17, 18, 21, 30. Cast No would be 1,2,3,4

    # combine station and event #, get unique of this combination while including Station 
    #    and Event columns
    # count events in station subsets

    # Unique subset of station and event

    # castno station event#
    # 1 P20  event 18
    # 2 P20  event 19
    # 3 P20  event 26


    # Steps. Loop through each station and do following
    #  Say station looking at is P20
    #  Get rows for that single station. All rows with station = P20

    # Now for that single station set of rows, filter event column
    # so only have incrementing list of event numbers

    # Now remap this incrementing event list to start at 1
    # and this will be the cast number


    # Sort by station and then event and reset index to match
    df.sort_values(by=['STATION','EVENT'],inplace=True)
    df.reset_index(drop=True,inplace=True)


    # Get unique values of STATION column
    station_df = df['STATION'].copy()
    unique_station_df = station_df.drop_duplicates()

    # Get list of unique stations
    unique_station_list = unique_station_df.tolist() 

    # For each station in list, populate their castno rows
    for station in unique_station_list:

        # Populate castno for station
        df = populate_castno_one_station(df,station)

    return df


def insert_station_castno_column(df):

    # Create STATION_CAST column which is a combo of station and castno to 
    # make it easy to sort and group on this value for each station to save
    # each to a file
    df['STATION_CASTNO'] = df['STATION'].apply(str) + '_' + df['CASTNO'].apply(str)

    return df


def get_data_columns(df, data_params):

    # Get list of columns from params
    col_list = []

    for param in data_params:

        col_list.append(param['whpname'])

    # Get params set of columns from df
    df = df[col_list].copy()

    return df


def get_unique_station_castno_sets(df):

    # Get unique values of STATION_CASTNO column
    station_castno_df = df['STATION_CASTNO'].copy()
    unique_station_castno_df = station_castno_df.drop_duplicates()
    unique_station_castno_df.reset_index(drop=True,inplace=True)

    return unique_station_castno_df


def get_station_castno_df_sets(df, unique_station_castno_df):

    # From list of unique station_castno combinations, get that
    # subset and append a list containing all these subsets.
    # Breaks up station dataframe into subsets of unique station/castno
    # that will be saved to its own file

    station_df_sets = []

    # Get list of unique station castno dataframes
    unique_station_castno_list = unique_station_castno_df.tolist()

    # Convert unique_station_castno_df to a list
    # For each station_castno in list, get subset of df for it
    # and append to station_df_sets

    for station_castno in unique_station_castno_list:

        df_subset = df.loc[df['STATION_CASTNO'] == station_castno].copy()
        df_subset.reset_index(drop=True,inplace=True)

        station_df_sets.append(df_subset)

    return station_df_sets
