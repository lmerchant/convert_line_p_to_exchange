import pytest
import pandas as pd

from convert_line_p_to_exchange.utilities.data_files import *
import convert_line_p_to_exchange.utilities.process_raw_data


def test_create_start_end_lines(monkeypatch):

    NOW_TIME = datetime.datetime(2020, 12, 25, 17, 5, 55)

    class mydatetime:
        def now(self):
            return NOW_TIME


    monkeypatch.setattr(datetime, 'datetime', mydatetime())

    expected_start_line = start_line = "CTD,20201225CCHSIOLM"
    expected_end_line = 'END_DATA'


    real_start_line, real_end_line = create_start_end_lines()

    assert real_start_line == expected_start_line

    assert real_end_line == expected_end_line
    

@pytest.fixture
def station_variation_sets():

    # STATION = 'P4'
    column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'EXPOCODE', 'STATION', 'CASTNO', 'INS:LOCATION', 'CTDPRS']

    data = [['2017-01-0017.ctd', 'UTC', '20170207', '1640', '18', '48.6485', '-126.667', '<expocode>', 'P4', '10', 'Mid-ship', '2'], ['2017-01-0017.ctd', 'UTC', '20170207', '1640', '18', '48.6485', '-126.667', '<expocode>', 'P4', '10', 'Mid-ship', '2']]

    testing_df1 = pd.DataFrame(data, columns = column_names)

    # STATION = 'P1/B8'
    column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'EXPOCODE', 'STATION', 'CASTNO', 'INS:LOCATION', 'CTDPRS']

    data = [['2017-01-0017.ctd', 'UTC', '20170207', '1640', '18', '48.6485', '-126.667', '<expocode>', 'P1/B8', '100', 'Mid-ship', '2'], ['2017-01-0017.ctd', 'UTC', '20170207', '1640', '18', '48.6485', '-126.667', '<expocode>', 'P1/B8', '100', 'Mid-ship', '2']]

    testing_df2 = pd.DataFrame(data, columns = column_names)

    # STATION = 'PA-001'
    column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'EXPOCODE', 'STATION', 'CASTNO', 'INS:LOCATION', 'CTDPRS']

    data = [['2017-01-0017.ctd', 'UTC', '20170207', '1640', '18', '48.6485', '-126.667', '<expocode>', 'PA-001', '1', 'Mid-ship', '2'], ['2017-01-0017.ctd', 'UTC', '20170207', '1640', '18', '48.6485', '-126.667', '<expocode>', 'PA-001', '1', 'Mid-ship', '2']]

    testing_df3 = pd.DataFrame(data, columns = column_names)

    return (testing_df1, testing_df2, testing_df3)


def test_get_ctd_filename(station_variation_sets):

    testing_df1, testing_df2, testing_df3 = station_variation_sets

    directory = 'data/<expocode>_ct1/'

    expected_filename1 = 'data/<expocode>_ct1/<expocode>_000P4_00010_ct1.csv'

    real_filename1 = get_ctd_filename(directory, testing_df1)

    assert real_filename1 == expected_filename1


    expected_filename2 = 'data/<expocode>_ct1/<expocode>_P1-B8_00100_ct1.csv'

    real_filename2 = get_ctd_filename(directory, testing_df2)

    assert real_filename2 == expected_filename2


    expected_filename3 = 'data/<expocode>_ct1/<expocode>_PA001_00001_ct1.csv'

    real_filename3 = get_ctd_filename(directory, testing_df3)

    assert real_filename3 == expected_filename3


def test_get_individual_raw_file(monkeypatch):

    column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'EXPOCODE', 'STATION', 'CASTNO', 'INS:LOCATION', 'CTDPRS']

    data = [['2017-01-0017.ctd', 'UTC', '20170207', '1640', '18', '48.6485', '-126.667', '<expocode>', 'P4', '10', 'Mid-ship', '2'], ['2017-01-0017.ctd', 'UTC', '20170207', '1640', '18', '48.6485', '-126.667', '<expocode>', 'P4', '10', 'Mid-ship', '2']]

    testing_df1 = pd.DataFrame(data, columns = column_names)

    def mock_get_cruise_list():
        return [('<line_p_year>', '<line_p_id>', '<expocode>')]

    monkeypatch.setattr(convert_line_p_to_exchange.utilities.process_raw_data, 'get_cruise_list', mock_get_cruise_list)

    def mock_get_ids(expocode, cruise_list):
        return '<line_p_year>', '<line_p_id>'

    monkeypatch.setattr(convert_line_p_to_exchange.utilities.data_files,'get_line_p_cruise_id', mock_get_ids)

    expected_url = 'https://www.waterproperties.ca/linep/<line_p_year>-<line_p_id>/donneesctddata/<line_p_year>-<line_p_id>-0018.ctd'

    expocode = '<expocode>'

    real_url = get_individual_raw_file(expocode, testing_df1)

    assert real_url == expected_url


def test_get_individual_raw_file_header(monkeypatch):

    testing_header = ["*2014/12/02 11:39:47.13",
        "*IOS HEADER VERSION 1.10 2011/10/26 2011/10/26",
        "",
        "*FILE",
        "    START TIME          : UTC 2007/02/09 10:20:43.000",
        "!--1--- ---2---- ---3---- ---4---- --5-- --6--- --7-- ---8---",
        "!Pressu Temperat Salinity Sigma-t: Trans Oxygen Oxyge Fluores",
        "!re:CTD ure:CTD  :CTD     CTD      missi :Disso n:Dis cence: ",
        "!                                  vity: lved:  solve CTD:   ",
        "!                                  CTD   CTD:   d:CTD Seapoin",
        "!                                        Volume :Mass t      ",
        "!------ -------- -------- -------- ----- ------ ----- -------",
        "*END OF HEADER",
        "    4.3   8.4168  31.9619  24.8539  45.3   6.43 280.1   0.619"]

    expected_header = ["#*2014/12/02 11:39:47.13",
        "#*IOS HEADER VERSION 1.10 2011/10/26 2011/10/26",
        "#",
        "#*FILE",
        "#    START TIME          : UTC 2007/02/09 10:20:43.000",
        "#!--1--- ---2---- ---3---- ---4---- --5-- --6--- --7-- ---8---",
        "#!Pressu Temperat Salinity Sigma-t: Trans Oxygen Oxyge Fluores",
        "#!re:CTD ure:CTD  :CTD     CTD      missi :Disso n:Dis cence: ",
        "#!                                  vity: lved:  solve CTD:   ",
        "#!                                  CTD   CTD:   d:CTD Seapoin",
        "#!                                        Volume :Mass t      ",
        "#!------ -------- -------- -------- ----- ------ ----- -------",
        "#*END OF HEADER"]



    def mock_get_file_text(url):
        return testing_header

    monkeypatch.setattr(convert_line_p_to_exchange.utilities.data_files, 'get_file_text', mock_get_file_text)

    url = 'https://...'

    real_header = get_individual_raw_file_header(url)

    assert real_header == expected_header


@pytest.mark.parametrize("EXCEPTION_FLAG", [False, True])
def test_choose_raw_file_header(monkeypatch, EXCEPTION_FLAG):

    # Test for cases of url found and not found

    def mock_get_file_header(url):
        if EXCEPTION_FLAG:
            raise Exception('url not found')
        else:
            return ['# Comment Header Line 1', '# Comment Header Line 2']

    monkeypatch.setattr(convert_line_p_to_exchange.utilities.data_files, 'get_individual_raw_file_header', mock_get_file_header)

    # url either of individual file by event number, a corrected individ file, or no individ file found
    url1 = "https://individual_file"
    url2 = "https://www.waterproperties.ca/linep/2009-09/donneesctddata/2009-09-0014.ctd"  
    url3 = "https://individual_file_not_found"  

    comment_header = ["comment"]
    ctd_filename = "ctd_filename"


    # Case 1: url is of individual file by event number
    real_header = choose_raw_file_header(url1, comment_header, ctd_filename)

    if EXCEPTION_FLAG:
        expected_header = ["comment"]
    else:
        expected_header = ['# Comment Header Line 1', '# Comment Header Line 2']

    assert real_header == expected_header


    # Case 2: url is a correction of individual file by event number
    #         and is searched for only if an exception occurs
    try:
        real_header = choose_raw_file_header(url2, comment_header, ctd_filename)
        expected_header = ['# Comment Header Line 1', '# Comment Header Line 2']
    except:
        real_header = ['# Comment Header Line 1', '# Comment Header Line 2']
        expected_header = ['# Comment Header Line 1', '# Comment Header Line 2']

    assert real_header == expected_header


    # Case 3: url of an individual file by event number not found
    # and is searched for only if an exception occurs

    real_header = choose_raw_file_header(url3, comment_header, ctd_filename)

    if EXCEPTION_FLAG:
        expected_header = ["comment"]
    else:
        expected_header = ['# Comment Header Line 1', '# Comment Header Line 2']

    assert real_header == expected_header





