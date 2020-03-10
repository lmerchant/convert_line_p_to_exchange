import pytest
import pandas as pd
from pandas.util.testing import assert_frame_equal, assert_series_equal

from utilities.data_columns import *


def test_rename_pline_columns(comment_column_names_data_from_data_with_one_row):

    comment_header, column_names, data = comment_column_names_data_from_data_with_one_row

    df = pd.DataFrame(data, columns=column_names)

    meta_params =[
    {'whpname' : 'DATE' , 'longname':'FIL:START TIME YYYY/MM/DD', 'units' : ''},                       
    {'whpname' : 'TIME' , 'longname':' HH:MM', 'units' : ''},
    {'whpname' : 'EVENT' , 'longname' : 'LOC:EVENT_NUMBER', 'units' : ''},                              
    {'whpname' : 'STATION' , 'longname':'LOC:STATION', 'units' : ''},                                  
    {'whpname' : 'LATITUDE' , 'longname':'LOC:LATITUDE', 'units' : ''},                               
    {'whpname' : 'LONGITUDE' , 'longname':'LOC:LONGITUDE', 'units' : ''},                             
    ]  

    data_params =[                             
    {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},
    {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
    {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
    {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},   
    {'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [*/m]', 'units' : '/METER'},
    {'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'},   
    {'whpname' : 'CTDFLUOR_TSG' , 'longname':'Fluorescence:CTD:Wetlabs [mg/m^3]', 'units' : 'MG/M^3'},
    {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}     
    ]  

    expected_columns = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'STATION', 'INS:LOCATION', 'CTDPRS', 'CTDTMP', 'CTDSAL', 'Sigma-t:CTD [kg/m^3]', 'CTDBEAMCP', 'Oxygen:Dissolved:CTD:Volume [ml/l]', 'CTDOXY', 'CTDFLUOR', 'CTDFLUOR_TSG', 'PAR']

    expected_df = pd.DataFrame(data, columns=expected_columns)

    real_df = rename_pline_columns(df, meta_params, data_params)

    assert_frame_equal(real_df, expected_df)


def test_insert_flag_colums():
                 
    expected_data = [['2017-01-0017.ctd', '2',2,'9.0615',2, '32.3599',2, '25.0678', '',2, '6.43', '280',2, '0.714',2, '',2, '64', 2]]

    expected_columns = ['File Name', 'CTDPRS', 'CTDPRS_FLAG_W', 'CTDTMP', 'CTDTMP_FLAG_W', 'CTDSAL', 'CTDSAL_FLAG_W', 'Sigma-t:CTD [kg/m^3]', 'CTDBEAMCP', 'CTDBEAMCP_FLAG_W', 'Oxygen:Dissolved:CTD:Volume [ml/l]', 'CTDOXY', 'CTDOXY_FLAG_W', 'CTDFLUOR', 'CTDFLUOR_FLAG_W', 'CTDFLUOR_TSG','CTDFLUOR_TSG_FLAG_W', 'PAR', 'PAR_FLAG_W']

    expected_df = pd.DataFrame(expected_data, columns=expected_columns)


    data_params =[                             
    {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},
    {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
    {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
    {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},   
    {'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [*/m]', 'units' : '/METER'},
    {'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'},   
    {'whpname' : 'CTDFLUOR_TSG' , 'longname':'Fluorescence:CTD:Wetlabs [mg/m^3]', 'units' : 'MG/M^3'},
    {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}     
    ]     

    testing_column_names = ['File Name', 'CTDPRS', 'CTDTMP', 'CTDSAL', 'Sigma-t:CTD [kg/m^3]', 'CTDBEAMCP', 'Oxygen:Dissolved:CTD:Volume [ml/l]', 'CTDOXY', 'CTDFLUOR', 'CTDFLUOR_TSG', 'PAR']

    # Expect list of lists
    testing_data = [['2017-01-0017.ctd', '2', '9.0615', '32.3599', '25.0678', '', '6.43', '280', '0.714', '', '64']]

    testing_df = pd.DataFrame(testing_data, columns=testing_column_names)

    real_df, real_params = insert_flag_colums(testing_df, data_params)

    assert_frame_equal(real_df, expected_df)


def test_update_flag_for_fill_999_str():

    testing_data = [['2017-01-0017.ctd', '2',2,'9.0615',2, '32.3599',2, '25.0678', '',2, '6.43', '280',2, '0.714',2, '-999',2, '64', 2]]

    testing_columns = ['File Name', 'CTDPRS', 'CTDPRS_FLAG_W', 'CTDTMP', 'CTDTMP_FLAG_W', 'CTDSAL', 'CTDSAL_FLAG_W', 'Sigma-t:CTD [kg/m^3]', 'CTDBEAMCP', 'CTDBEAMCP_FLAG_W', 'Oxygen:Dissolved:CTD:Volume [ml/l]', 'CTDOXY', 'CTDOXY_FLAG_W', 'CTDFLUOR', 'CTDFLUOR_FLAG_W', 'CTDFLUOR_TSG','CTDFLUOR_TSG_FLAG_W', 'PAR', 'PAR_FLAG_W']

    testing_df = pd.DataFrame(testing_data, columns=testing_columns) 

    # Expect flag for -999 value to turn from 2 to 9
    expected_data = [['2017-01-0017.ctd', '2',2,'9.0615',2, '32.3599',2, '25.0678', '',2, '6.43', '280',2, '0.714',2, '-999',9, '64', 2]]

    expected_df = pd.DataFrame(expected_data, columns=testing_columns)

    testing_data_params = [                             
        {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},
        {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
        {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
        {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},   
        {'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [*/m]', 'units' : '/METER'},
        {'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'},   
        {'whpname' : 'CTDFLUOR_TSG' , 'longname':'Fluorescence:CTD:Wetlabs [mg/m^3]', 'units' : 'MG/M^3'},
        {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}     
        ] 

    real_df = update_flag_for_fill_999_str(testing_df, testing_data_params)

    assert_frame_equal(real_df, expected_df)


def test_update_flag_for_fill_99_str():

    testing_columns = ['File Name', 'CTDPRS', 'CTDPRS_FLAG_W', 'CTDTMP', 'CTDTMP_FLAG_W', 'CTDSAL', 'CTDSAL_FLAG_W', 'Sigma-t:CTD [kg/m^3]', 'CTDBEAMCP', 'CTDBEAMCP_FLAG_W', 'Oxygen:Dissolved:CTD:Volume [ml/l]', 'CTDOXY', 'CTDOXY_FLAG_W', 'CTDFLUOR', 'CTDFLUOR_FLAG_W', 'CTDFLUOR_TSG','CTDFLUOR_TSG_FLAG_W', 'PAR', 'PAR_FLAG_W']

    testing_data = [['2017-01-0017.ctd', '2',2,'9.0615',2, '32.3599',2, '25.0678', '',2, '6.43', '280',2, '0.714',2, '-99',2, '64', 2]]

    testing_df = pd.DataFrame(testing_data, columns=testing_columns) 

    # Expect flag for -999 value to turn from 2 to 9
    expected_data = [['2017-01-0017.ctd', '2',2,'9.0615',2, '32.3599',2, '25.0678', '',2, '6.43', '280',2, '0.714',2, '-99',5, '64', 2]]

    expected_df = pd.DataFrame(expected_data, columns=testing_columns)


    testing_data_params = [                             
        {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},
        {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
        {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
        {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},   
        {'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [*/m]', 'units' : '/METER'},
        {'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'},   
        {'whpname' : 'CTDFLUOR_TSG' , 'longname':'Fluorescence:CTD:Wetlabs [mg/m^3]', 'units' : 'MG/M^3'},
        {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}     
        ] 

    real_df = update_flag_for_fill_99_str(testing_df, testing_data_params)

    assert_frame_equal(real_df, expected_df)


def test_replace_fill_values_in_df():

    """
    Test replacing any -99, -99.0, -999.0 values with exchange fill of -999
    after turning all values into strings
    """

    testing_columns = ['File Name', 'CTDPRS', 'CTDPRS_FLAG_W', 'CTDTMP', 'CTDTMP_FLAG_W', 'CTDSAL', 'CTDSAL_FLAG_W', 'Sigma-t:CTD [kg/m^3]', 'CTDBEAMCP', 'CTDBEAMCP_FLAG_W', 'Oxygen:Dissolved:CTD:Volume [ml/l]', 'CTDOXY', 'CTDOXY_FLAG_W', 'CTDFLUOR', 'CTDFLUOR_FLAG_W', 'CTDFLUOR_TSG','CTDFLUOR_TSG_FLAG_W', 'PAR', 'PAR_FLAG_W']

    testing_data = [['2017-01-0017.ctd', 2, 2,'9.0615',2, '32.3599',2, '999', '',2, '99', '-99.0',2, '-99',2, '-999.0',2, '-999', 2]]

    testing_df = pd.DataFrame(testing_data, columns=testing_columns) 

    expected_data = [['2017-01-0017.ctd', '2', '2','9.0615','2', '32.3599','2', '999', '','2', '99', '-999','2', '-999','2', '-999','2', '-999', '2']]

    expected_df = pd.DataFrame(expected_data, columns=testing_columns)


    real_df = replace_fill_values_in_df(testing_df)

    assert_frame_equal(real_df, expected_df)


@pytest.fixture
def testing_different_date_formats():

    # Given different date formats, want final form to be exchange format yyyymmdd

    column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'STATION', 'INS:LOCATION', ]

    # Date of form dd-mm-yy 
    data = [['2017-01-0017.ctd', 'UTC', '07-02-17', ' 16:40', '17', '48.6485', '-126.667', 'P4', 'Mid-ship']]
    
    testing_df_set1 = pd.DataFrame(data, columns=column_names)

    # Date of form dd/mm/yyyy 
    data = [['2017-01-0017.ctd', 'UTC', '07/02/2017', ' 16:40', '17', '48.6485', '-126.667', 'P4', 'Mid-ship']]
    
    testing_df_set2 = pd.DataFrame(data, columns=column_names)

    # Date of form yyyy/mm/dd
    data = [['2017-01-0017.ctd', 'UTC', '2017/02/07', ' 16:40', '17', '48.6485', '-126.667', 'P4', 'Mid-ship']]
    
    testing_df_set3 = pd.DataFrame(data, columns=column_names)    


    expected_data = [['2017-01-0017.ctd', 'UTC', '20170207', ' 16:40', '17', '48.6485', '-126.667', 'P4', 'Mid-ship']]
    expected_df = pd.DataFrame(expected_data, columns=column_names)

    return expected_df, (testing_df_set1, testing_df_set2, testing_df_set3)


def test_reformat_date_column(testing_different_date_formats):

    # Given different date formats, want final form to be exchange format yyyymmdd

    expected_df, testing_df_sets = testing_different_date_formats

    for testing_df in testing_df_sets:

        real_df = reformat_date_column(testing_df)

        assert_frame_equal(real_df, expected_df)



@pytest.fixture
def testing_different_time_formats():

    # Reformat time column from HH:MM to HHMM

    column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'STATION', 'INS:LOCATION', ]

    # Time of form hh:mm with single hour 
    data = [['2017-01-0017.ctd', 'UTC', '07-02-17', ' 08:40', '17', '48.6485', '-126.667', 'P4', 'Mid-ship']]
    expected_data = [['2017-01-0017.ctd', 'UTC', '07-02-17', '0840', '17', '48.6485', '-126.667', 'P4', 'Mid-ship']]
    
    testing_df_set1 = pd.DataFrame(data, columns=column_names)
    expected_df_set1 = pd.DataFrame(expected_data, columns=column_names)

    # Time of form hh:mm with double hour 
    data = [['2017-01-0017.ctd', 'UTC', '07-02-17', ' 16:40', '17', '48.6485', '-126.667', 'P4', 'Mid-ship']]
    expected_data = [['2017-01-0017.ctd', 'UTC', '07-02-17', '1640', '17', '48.6485', '-126.667', 'P4', 'Mid-ship']]
   
    testing_df_set2 = pd.DataFrame(data, columns=column_names)
    expected_df_set2 = pd.DataFrame(expected_data, columns=column_names)

    testing_df_sets = (testing_df_set1, testing_df_set2)
    expected_df_sets = (expected_df_set1, expected_df_set2)

    return expected_df_sets, testing_df_sets


def test_reformat_time_column(testing_different_time_formats):

    # Reformat time column from HH:MM to HHMM

    expected_df_sets, testing_df_sets = testing_different_time_formats

    for expected_df, testing_df in zip(expected_df_sets, testing_df_sets):

        real_df = reformat_time_column(testing_df)

        assert_frame_equal(real_df, expected_df)


def test_insert_expocode_column():

    # Insert expocode column before STATION column

    column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'STATION', 'INS:LOCATION', ]

    # Time of form hh:mm with single hour 
    data = [['2017-01-0017.ctd', 'UTC', '07-02-17', '0840', '17', '48.6485', '-126.667', 'P4', 'Mid-ship']]

    testing_df = pd.DataFrame(data, columns = column_names)


    expected_column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'EXPOCODE', 'STATION', 'INS:LOCATION', ]

    expected_data = [['2017-01-0017.ctd', 'UTC', '07-02-17', '0840', '17', '48.6485', '-126.667', '<expocode>', 'P4', 'Mid-ship']]

    expected_df = pd.DataFrame(expected_data, columns=expected_column_names)

    expocode = '<expocode>'

    real_df = insert_expocode_column(testing_df, expocode)

    assert_frame_equal(real_df, expected_df)


def test_populate_castno_one_station():

    # Incoming df has been sorted on station and event number

    column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'STATION', 'CASTNO', 'INS:LOCATION']

    # Time of form hh:mm with single hour 
    data = [['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P3', 0, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 15, '48.6485', '-126.667', 'P3', 0, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 10, '48.6485', '-126.667', 'P4', 0, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P4', 0, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 12, '48.6485', '-126.667', 'P4', 0, 'Mid-ship']]

    testing_df = pd.DataFrame(data, columns = column_names)

    expected_data = [['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P3', 0, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 15, '48.6485', '-126.667', 'P3', 0, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 10, '48.6485', '-126.667', 'P4', 1, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship']]

    expected_df = pd.DataFrame(expected_data, columns = column_names)

    station = 'P4'

    real_df = populate_castno_one_station(testing_df, station)

    assert_frame_equal(real_df, expected_df)


def test_insert_castno_column():

    column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'STATION', 'INS:LOCATION']

    # Time of form hh:mm with single hour 
    data = [['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P3', 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 15, '48.6485', '-126.667', 'P3', 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 10, '48.6485', '-126.667', 'P4', 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P4', 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 12, '48.6485', '-126.667', 'P4', 'Mid-ship']]

    testing_df = pd.DataFrame(data, columns = column_names)

    expected_column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'STATION', 'CASTNO', 'INS:LOCATION']

    expected_data = [['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P3', 1, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 15, '48.6485', '-126.667', 'P3', 2, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 10, '48.6485', '-126.667', 'P4', 1, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship']]

    expected_df = pd.DataFrame(expected_data, columns = expected_column_names)

    real_df = insert_castno_column(testing_df)

    assert_frame_equal(real_df, expected_df)


def test_insert_station_castno_column():

    column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'STATION', 'CASTNO', 'INS:LOCATION']

    data = [['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P3', 1, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 15, '48.6485', '-126.667', 'P3', 2, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 10, '48.6485', '-126.667', 'P4', 1, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship']]

    testing_df = pd.DataFrame(data, columns = column_names)

    # Insert new column 'STATION_CASTNO' at end of data frame
    expected_column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'STATION', 'CASTNO', 'INS:LOCATION','STATION_CASTNO']

    expected_data = [['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P3', 1, 'Mid-ship', 'P3_1'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 15, '48.6485', '-126.667', 'P3', 2, 'Mid-ship', 'P3_2'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 10, '48.6485', '-126.667', 'P4', 1, 'Mid-ship', 'P4_1'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship', 'P4_2'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship', 'P4_2']]

    expected_df = pd.DataFrame(expected_data, columns = expected_column_names)

    real_df = insert_station_castno_column(testing_df)

    assert_frame_equal(real_df, expected_df)


def test_get_data_columns():

    column_names = ['File Name', 'CTDPRS', 'CTDPRS_FLAG_W', 'CTDTMP', 'CTDTMP_FLAG_W', 'CTDSAL', 'CTDSAL_FLAG_W', 'Sigma-t:CTD [kg/m^3]', 'CTDBEAMCP', 'CTDBEAMCP_FLAG_W', 'Oxygen:Dissolved:CTD:Volume [ml/l]', 'CTDOXY', 'CTDOXY_FLAG_W', 'CTDFLUOR', 'CTDFLUOR_FLAG_W', 'CTDFLUOR_TSG','CTDFLUOR_TSG_FLAG_W', 'PAR', 'PAR_FLAG_W']

    data = [['2017-01-0017.ctd', '2',2,'9.0615',2, '32.3599',2, '25.0678', '',2, '6.43', '280',2, '0.714',2, '',2, '64', 2]]

    testing_df = pd.DataFrame(data, columns = column_names)

    testing_data_params = [                             
        {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},
        {'whpname' : 'CTDPRS_FLAG_W', 'longname': 'CTDPRS_FLAG_W', 'units' : ''},
        {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
        {'whpname' : 'CTDTMP_FLAG_W', 'longname': 'CTDTMP_FLAG_W', 'units' : ''},
        {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
        {'whpname' : 'CTDSAL_FLAG_W', 'longname': 'CTDSAL_FLAG_W', 'units' : ''},
        {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},
        {'whpname' : 'CTDOXY_FLAG_W', 'longname': 'CTDOXY_FLAG_W', 'units' : ''},
        {'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [*/m]', 'units' : '/METER'},
        {'whpname' : 'CTDBEAMCP_FLAG_W', 'longname': 'CTDBEAMCP_FLAG_W', 'units' : ''},
        {'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'}, 
        {'whpname' : 'CTDFLUOR_FLAG_W', 'longname': 'CTDFLUOR_FLAG_W', 'units' : ''},
        {'whpname' : 'CTDFLUOR_TSG' , 'longname':'Fluorescence:CTD:Wetlabs [mg/m^3]', 'units' : 'MG/M^3'},
        {'whpname' : 'CTDFLUOR_TSG_FLAG_W', 'longname': 'CTDFLUOR_TSG_FLAG_W', 'units' : ''},
        {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'} ,
        {'whpname' : 'PAR_FLAG_W', 'longname': 'PAR_FLAG_W', 'units' : ''}
        ]     

    testing_df = pd.DataFrame(data, columns = column_names)


    expected_column_names = ['CTDPRS', 'CTDPRS_FLAG_W', 'CTDTMP', 'CTDTMP_FLAG_W', 'CTDSAL', 'CTDSAL_FLAG_W', 'CTDOXY', 'CTDOXY_FLAG_W', 'CTDBEAMCP', 'CTDBEAMCP_FLAG_W', 'CTDFLUOR', 'CTDFLUOR_FLAG_W', 'CTDFLUOR_TSG','CTDFLUOR_TSG_FLAG_W', 'PAR', 'PAR_FLAG_W']

    expected_data = [['2',2,'9.0615',2, '32.3599',2,'280',2,'',2, '0.714',2, '',2, '64', 2]]

    expected_df = pd.DataFrame(expected_data, columns = expected_column_names)

    real_df = get_data_columns(testing_df, testing_data_params)

    assert_frame_equal(real_df, expected_df)


def get_unique_station_castno_sets():

    # Get unique values of STATION_CASTNO column

    column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'STATION', 'CASTNO', 'INS:LOCATION','STATION_CASTNO']

    data = [['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P3', 1, 'Mid-ship', 'P3_1'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 15, '48.6485', '-126.667', 'P3', 2, 'Mid-ship', 'P3_2'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 10, '48.6485', '-126.667', 'P4', 1, 'Mid-ship', 'P4_1'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship', 'P4_2'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship', 'P4_2']]

    testing_df = pd.DataFrame(data, columns = column_names)    

    expected_data = ['P3_1', 'P3_2', 'P4_1', 'P4_2']

    expected_df = pd.Series(expected_data)
    expected_df = expected_df.rename("STATION_CASTNO")

    real_df = get_unique_station_castno_sets(testing_df)

    assert_series_equal(real_df, expected_df)


def test_get_station_castno_df_sets():

    column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'STATION', 'CASTNO', 'INS:LOCATION','STATION_CASTNO']

    data = [['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P3', 1, 'Mid-ship', 'P3_1'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 15, '48.6485', '-126.667', 'P3', 2, 'Mid-ship', 'P3_2'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 10, '48.6485', '-126.667', 'P4', 1, 'Mid-ship', 'P4_1'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship', 'P4_2'],
    ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship', 'P4_2']]

    testing_df = pd.DataFrame(data, columns = column_names)

    unique_station_castno_data = ['P3_1', 'P3_2', 'P4_1', 'P4_2']

    testing_unique_station_castno_df = pd.Series(unique_station_castno_data)
    testing_unique_station_castno_df = testing_unique_station_castno_df.rename("STATION_CASTNO")  


    expected_df_sets = []  

    expected_subset1 = [['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P3', 1, 'Mid-ship', 'P3_1']]
    expected_subset_df1 = pd.DataFrame(expected_subset1, columns=column_names)
    expected_df_sets.append(expected_subset_df1)

    expected_subset2 = [['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 15, '48.6485', '-126.667', 'P3', 2, 'Mid-ship', 'P3_2']]
    expected_subset_df2 = pd.DataFrame(expected_subset2, columns=column_names)
    expected_df_sets.append(expected_subset_df2)

    expected_subset3 = [['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 10, '48.6485', '-126.667', 'P4', 1, 'Mid-ship', 'P4_1']]
    expected_subset_df3 = pd.DataFrame(expected_subset3, columns=column_names)
    expected_df_sets.append(expected_subset_df3)

    expected_subset4 = [['2017-01-0017.ctd', 'UTC', '07-02-17', '08:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship', 'P4_2'],
                        ['2017-01-0017.ctd', 'UTC', '07-02-17', '09:40', 12, '48.6485', '-126.667', 'P4', 2, 'Mid-ship', 'P4_2']]
    expected_subset_df4 = pd.DataFrame(expected_subset4, columns=column_names)
    expected_df_sets.append(expected_subset_df4)


    real_df_sets = get_station_castno_df_sets(testing_df, testing_unique_station_castno_df)


    for real_df_set, expected_df_set in zip(real_df_sets, expected_df_sets):

        assert_frame_equal(real_df_set, expected_df_set)


