import pytest
import os
import pandas as pd
from pandas.util.testing import assert_frame_equal
import numpy as np

from convert_line_p_data_to_exchange.utilities.process_raw_data import *
import convert_line_p_data_to_exchange.utilities

from config import Config


SKIP_URL_DOWNLOAD = True


def test_get_cruise_list():

    cruise_list = get_cruise_list()
    first_element = cruise_list[0]

    assert isinstance(cruise_list, list)
    assert len(cruise_list) != 0
    assert isinstance(first_element, tuple)
    assert len(first_element) == 3
    for el in first_element:
        assert isinstance(el, str)


def test_build_url():

    year = '2010'
    cruise_id = '13'
    expected_url = 'https://www.waterproperties.ca/linep/2010-13/donneesctddata/2010-13-ctd-cruise.csv'  

    real_url = build_url(year, cruise_id)

    assert real_url == expected_url


@pytest.mark.skipif(SKIP_URL_DOWNLOAD, reason = "Hitting real url takes time")
def test_get_raw_csv_from_url():
    url = 'https://www.waterproperties.ca/linep/2010-13/donneesctddata/2010-13-ctd-cruise.csv'

    real_csv = get_raw_csv_from_url(url)

    assert isinstance(real_csv, list)

    first_element = real_csv[0]

    assert isinstance(first_element, str)


def test_get_headers_and_data(data_with_one_row, comment_column_names_data_from_data_with_one_row, monkeypatch):

    exp_comment_header, exp_column_names, exp_data = comment_column_names_data_from_data_with_one_row

    def mock_get_raw_csv(url):
        return data_with_one_row

    monkeypatch.setattr(utilities.process_raw_data, 'get_raw_csv', mock_get_raw_csv)

    url = 'http://...'

    real_comment_header, real_column_names, real_data = get_headers_and_data(url)

    assert real_comment_header == exp_comment_header
    assert real_column_names == exp_column_names
    assert real_data == exp_data


def test_drop_empty_rows():

    column_names = ['File Name', 'Pressure:CTD [dbar]', 'Temperature:CTD [deg_C_(ITS90)]']

    data = [['2017-01-0017.ctd', '2', '9.0615'],['  ', '', ''], ['']]

    testing_df = pd.DataFrame(data, columns=column_names)

    expected_df = pd.DataFrame([['2017-01-0017.ctd', '2', '9.0615']], columns=column_names)

    real_df = drop_empty_rows(testing_df)

    assert_frame_equal(real_df, expected_df) 


def test_convert_event_pressure_to_numeric():

    column_names = ['File Name', 'LOC:EVENT_NUMBER', 'Pressure:CTD [dbar]']

    # Expect list of lists
    data = [['2017-01-0017.ctd', '17', '2']]

    starting_df = pd.DataFrame(data, columns=column_names)

    # Expect event to integer and pressure to float
    expected_data = [['2017-01-0017.ctd', 17, 2.0]]
    expected_df = pd.DataFrame(expected_data, columns=column_names)

    real_df = convert_event_pressure_to_numeric(starting_df)

    assert_frame_equal(real_df, expected_df)


def test_get_line_p_station_rows():

    column_names = ['File Name', 'LOC:STATION', 'Pressure:CTD [dbar]']
    
    data = [['2017-01-0017.ctd', 'P4', '2'],['2017-01-0017.ctd', 'M4', '2']]

    testing_df = pd.DataFrame(data, columns=column_names)

    expected_df = pd.DataFrame([['2017-01-0017.ctd', 'P4', '2']], columns=column_names)

    real_df = get_line_p_station_rows(testing_df)

    assert_frame_equal(real_df, expected_df)    


def test_replace_nan_or_empty_cells_with_999():

    column_names = ['File Name', 'Pressure:CTD [dbar]', 'Temperature:CTD [deg_C_(ITS90)]']

    data = [['2017-01-0017.ctd', '2', '9'],['2017-01-0017.ctd', '3', np.nan],['2017-01-0017.ctd', '4', '']]        
    testing_df = pd.DataFrame(data, columns=column_names)

    expected_df = pd.DataFrame([['2017-01-0017.ctd', '2', '9'],['2017-01-0017.ctd', '3', '-999'],['2017-01-0017.ctd', '4', '-999']], columns=column_names)

    expected_array = expected_df.to_numpy()

    real_df = replace_nan_or_empty_cells_with_999(testing_df)        

    real_array = real_df.to_numpy()


    assert real_array.all() == expected_array.all()


def test_sort_df():

    # Sort by station and then pressure and reset index to match new row order

    column_names = ['File Name', 'LOC:STATION', 'Pressure:CTD [dbar]']

    data = [['2017-01-0017.ctd', 'P21', 4.0], ['2017-01-0017.ctd', 'P21', 3.0], ['2017-01-0017.ctd', 'P2', 4.0], ['2017-01-0017.ctd', 'P2', 3.0], ['2017-01-0017.ctd', 'P21', 2.0]]    
    testing_df = pd.DataFrame(data, columns=column_names)

    expected_df = pd.DataFrame([ ['2017-01-0017.ctd', 'P2', 3.0], ['2017-01-0017.ctd', 'P2', 4.0], ['2017-01-0017.ctd', 'P21', 2.0], ['2017-01-0017.ctd', 'P21', 3.0] , ['2017-01-0017.ctd', 'P21', 4.0]], columns=column_names)

    real_df = sort_df(testing_df)

    assert_frame_equal(real_df, expected_df) 



