import re
from pandas.api.types import is_string_dtype


def rename_pline_columns(df, meta_params, data_params):

    """
    Rename all Line P metadata and parameter data column headers

    meta_params and data_params are mapping dicts
    between Line P column names (longname) and 
    WHP column names (whpname)
    """

    param_dict = {}

    for param in meta_params:
        param_dict[param['longname']] = param['whpname']    

    for param in data_params:
        param_dict[param['longname']] = param['whpname']

    df.rename(columns=param_dict, inplace=True)

    return df


# def insert_flag_colums_ver2(df, data_params):

#     # Don't insert flag column for pressure and temperature
#     # Not using since debate over whether to have flags or not
#     # Current exchange files use flags.

#     # If use this script, need to test still if correct.

#     # Will be adding flag columns to data_params and 
#     # having order of param, param_flag.
#     # Data params have order of column names to save,
#     # so order same with interleaving flag columns

#     # Don't add a flag for pressure and temperature
#     new_params = []

#     # Get location of column to insert flag column after.
#     # Insert flag column with value of 2
#     for param in data_params:

#         param_name = param['whpname']

#         param_loc = df.columns.get_loc(param_name)
#         flag_name = '{}_FLAG_W'.format(param_name)

#         if param_name is 'CTDPRS' or param_name is 'CTDTMP':
#             # no flag columns for pressure and temperature
#             new_params.append(param)

#         else:

#             # Insert in next column
#             # Will move all other columns to right
#             df.insert(param_loc + 1, flag_name, 2)

#             # Insert flag param dict into data_params list.
#             # Want this so know which columns to extract for saving output and
#             # keep order of newly inserted flag column after param column
#             param_insert = {}
#             param_insert['whpname'] = flag_name
#             param_insert['longname'] = flag_name
#             param_insert['units'] = ''

#             new_params.append(param)
#             new_params.append(param_insert)

#     return df, new_params


def insert_flag_colums(df, data_params):

    """
    Add flag columns to data_params with order of param, param_flag.
    """

    new_params = []

    # Get location of column to insert flag column after.
    # Insert flag column with value of 2
    for param in data_params:

        param_name = param['whpname']

        param_loc = df.columns.get_loc(param_name)
        flag_name = '{}_FLAG_W'.format(param_name)

        # Insert flag param dict into data_params list.
        # Want this so know which columns to extract for saving output and
        # keep order of newly inserted flag column after param column

        # Insert in next column
        # Will move all other columns to right
        df.insert(param_loc + 1, flag_name, 2)

        # Now insert flag values into params dict
        param_insert = {}
        param_insert['whpname'] = flag_name
        param_insert['longname'] = flag_name
        param_insert['units'] = ''

        new_params.append(param)
        new_params.append(param_insert)

    return df, new_params


# def update_flag_for_fill_999(df, data_params):

#     """
#     If cell has fill -999 or -999.0, change corresponding flag cell to 9
#     Flag = 9 means not sampled
#     """

#     # Go column by column. If flag column, no fill value, so will skip

#     for param in data_params:

#         param_name = param['whpname']
#         flag_name = '{}_FLAG_W'.format(param_name)
#         param_loc = df.columns.get_loc(param_name)
#         flag_loc = param_loc + 1

#         df_rows = df.loc[(df[param_name] == -999) | (df[param_name] == -999.0)]
#         number_of_rows = df.shape[0]

#         if not df_rows.empty:
  
#             df.loc[df[param_name] == -999, flag_name] = 9
#             df.loc[df[param_name] == -999.0, flag_name] = 9


def update_flag_for_fill_999_str(df, data_params):

    """
    If cell has string fill -999 or -999.0, change corresponding flag cell to 9
    Flag = 9 means not sampled
    """

    # Go column by column. If it's a flag column, no fill value, so will skip

    for param in data_params:

        param_name = param['whpname']
        flag_name = '{}_FLAG_W'.format(param_name)
        param_loc = df.columns.get_loc(param_name)
        flag_loc = param_loc + 1

        # Can't compare numeric column with string value
        # So skip numeric columns

        if is_string_dtype(df[param_name]):
            df_rows = df.loc[(df[param_name] == '-999') | (df[param_name] == '-999.0')]
            number_of_rows = df.shape[0]

            if not df_rows.empty:
      
                df.loc[df[param_name] == '-999', flag_name] = 9
                df.loc[df[param_name] == '-999.0', flag_name] = 9

    return df


# def update_flag_for_fill_99(df, data_params):

#     """
#     If cell has numeric fill -99 or -99.0, change corresponding flag cell to 5
#     Flag = 5 means Not reported
#     """

#     # Go column by column. If flag column, no fill value, so will skip

#     for param in data_params:

#         param_name = param['whpname']
#         flag_name = '{}_FLAG_W'.format(param_name)
#         param_loc = df.columns.get_loc(param_name)
#         flag_loc = param_loc + 1

#         df_rows = df.loc[(df[param_name] == -99) | (df[param_name] == -99.0)]
#         number_of_rows = df.shape[0]

#         if not df_rows.empty:
  
#             df.loc[df[param_name] == -99, flag_name] = 5
#             df.loc[df[param_name] == -99.0, flag_name] = 5


def update_flag_for_fill_99_str(df, data_params):

    """
    If cell has string fill '-99' or '-99.0', change corresponding flag cell to 5
    Flag = 5 means Not reported
    """

    # Go column by column. If it's a flag column, no fill value, so will skip

    for param in data_params:

        param_name = param['whpname']
        flag_name = '{}_FLAG_W'.format(param_name)
        param_loc = df.columns.get_loc(param_name)
        flag_loc = param_loc + 1

        # Can't compare numeric column with string value
        # So skip numeric columns

        if is_string_dtype(df[param_name]):

            df_rows = df.loc[(df[param_name] == '-99') | (df[param_name] == '-99.0')]
            number_of_rows = df.shape[0]

            if not df_rows.empty:
      
                df.loc[df[param_name] == '-99', flag_name] = 5
                df.loc[df[param_name] == '-99.0', flag_name] = 5

    return df


def replace_fill_values_in_df(data_columns_df):

    """
    Replace any -99, -99.0, -999.0 values with exchange fill of -999
    After converting all data in frame to a string.
    """

    # Convert data columns to string
    data_columns_df = data_columns_df.astype('str')


    # replace -999.0 with -999 which is exchange fill value (has to be integer)
    # replace -99.0 and -99 with -999
    data_columns_df.replace('-999.0', '-999', inplace=True)
    data_columns_df.replace('-99.0', '-999', inplace=True)
    data_columns_df.replace('-99', '-999', inplace=True)

    return data_columns_df                


def reformat_date_column(df):

    """
    Convert to exchange format yyyymmdd

    Files can have two different date formats
    dd-mm-yy or dd/mm/yyyyy

    In future, date format may be YYYY/MM/DD or YYYYMMDD
    In that case, create new test case and check regexp

    """

    # check if date format of type (dd-mm-yyyy)
    dash_pattern_4yr = r'(\d\d)-(\d\d)-(\d\d\d\d)'
    dash_regexp_4yr = re.compile(dash_pattern_4yr)    
    dash_match_4yr = dash_regexp_4yr.search(df['DATE'][0])

    # check if date format of type (dd-mm-yy)
    dash_pattern_2yr = r'(\d\d)-(\d\d)-(\d\d)'
    dash_regexp_2yr = re.compile(dash_pattern_2yr)    
    dash_match_2yr = dash_regexp_2yr.search(df['DATE'][0])

    # check if date format of type (dd/mm/yyyy)
    slash_pattern1 = r'(\d\d)/(\d\d)/(\d\d\d\d)'
    slash_regexp1 = re.compile(slash_pattern1)
    slash_match1 = slash_regexp1.search(df['DATE'][0])

    # check if date format of type (yyyy/mm/dd)
    slash_pattern2 = r'(\d\d\d\d)/(\d\d)/(\d\d)'
    slash_regexp2 = re.compile(slash_pattern2)
    slash_match2 = slash_regexp2.search(df['DATE'][0])


    if dash_match_4yr:

        # 1) Reformat DATE column from dd-mm-yyyy to yyyymmdd

        pattern = dash_pattern_4yr

        repl = r'\3\2\1'

    elif dash_match_2yr:

        # 1) Reformat DATE column from dd-mm-yy to yyyymmdd

        # Check if yy is less than 40. 
        #  if it is, prepend with 20
        #  if it isn't, prepend with 19

        year = df['DATE'][0][-2:]

        pattern = dash_pattern_2yr

        if int(year) < 40:
            repl = r'20\3\2\1'
        else:
            repl = r'19\3\2\1'

    elif slash_match1:

        # 2) Reformat DATE column from dd/mm/yyyy to yyyymmdd
        pattern = slash_pattern1
        repl = r'\3\2\1'

    elif slash_match2:

        # 3) Reformat DATE column from yyyy/mm/dd to yyyymmdd
        pattern = slash_pattern2
        repl = r'\1\2\3'        

    else:
        print('Date pattern of dd-mm-yy, dd-mm-yyyy, dd/mm/yyyy, or yyyy/mm/dd not found.') 
        print('Check if in exchange format (yyyymmdd) or not.')
        print('If not in exchange format, create a new regexp to fix date format.')

    df['DATE'] = df['DATE'].str.replace(pattern, repl)

    return df


def reformat_time_column(df):

    """
    # Reformat time column from HH:MM to HHMM
    """

    # Only need to use first time value since all the same
    # Remove beginning space
    if ':' in df['TIME'][0]:

        pattern = r'\s*(\d\d):(\d\d)'

        repl = r'\1\2'

    df['TIME'] = df['TIME'].str.replace(pattern, repl)

    return df


def insert_expocode_column(df, expocode):

    # TODO: check expocode column in right place
    # Should I use insert at STATION_LOC or
    # STATION_LOC - 1

    """
    Create EXPOCODE column
    """

    # Insert before STATION column
    STATION_LOC = df.columns.get_loc('STATION')

    df.insert(STATION_LOC, 'EXPOCODE', expocode)

    return df


def populate_castno_one_station(df, station):

    """
    Incoming df has been sorted on station and event number

    Want to get station subset and find all the unique event numbers
    used for that station. Then take these sorted event numbers and 
    map it to an increasing castno that starts at 1.
    Once have mapping of event numbers to castno, fill rows of castno column
    """

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
        # Since index starts at 0,increment by 1 to get CASTNO which starts at 1
        df.loc[(df.EVENT == event) & (df.STATION == station), 'CASTNO'] = index + 1  

    return df      


def insert_castno_column(df):

    """
    Creating castno from sorted list of unique event numbers
    for each station.  Mapping increasing event number to 
    increasing castno where castno starts at one and has increment of 1  
    """  

    # Create CASTNO column and fill with dummy value
    # Insert after STATION column    
    STATION_LOC = df.columns.get_loc('STATION')
    df.insert(STATION_LOC + 1, 'CASTNO', 0)

    # Will be filling the CASTNO column station by station.

    # castno is a mapping of the sequential event numbers of a single station
    # Say the event number series is 17, 18, 21, 30. Cast No would be 1,2,3,4

    # combine station and event #, get unique of this combination while including Station 
    #    and Event columns
    # count events in station subsets

    # Unique subset of station and event #

    # station   Event #    Cast #
    # P20       event 18   1
    # P20       event 19   2
    # P20       event 26   3


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

    """
    Create STATION_CASTNO column which is a combo of station and castno to 
    make it easy to sort and group on this value for each station to save
    each to a file
    """

    df['STATION_CASTNO'] = df['STATION'].apply(str) + '_' + df['CASTNO'].apply(str)

    return df


def get_data_columns(df, data_params):

    """
    Get subset of columns that are in listed in data_params.
    Use whpname for columns
    """

    # Get list of columns from params
    col_list = []

    # Get column list in order wanted for df
    for param in data_params:

        col_list.append(param['whpname'])

    # Get params set of columns from df (subset of columns)
    df = df[col_list].copy()

    return df


def get_unique_station_castno_sets(df):

    """
    Get unique values of STATION_CASTNO column
    """

    station_castno_df = df['STATION_CASTNO'].copy()
    unique_station_castno_df = station_castno_df.drop_duplicates()
    unique_station_castno_df.reset_index(drop=True,inplace=True)

    return unique_station_castno_df


def get_station_castno_df_sets(df, unique_station_castno_df):

    """
    From list of unique station_castno combinations, get that
    subset and append a list containing all these subsets.
    Breaks up station dataframe into subsets of unique station/castno
    that will be saved to its own file
    """

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

