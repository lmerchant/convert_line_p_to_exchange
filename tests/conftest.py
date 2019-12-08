import pytest
from pathlib import Path
import pandas as pd


@pytest.fixture
def data_with_one_row():
    filepath = Path(__file__).parents[0] / 'data' / 'data_to_test_one_row.csv'

    with open(filepath, 'r', encoding='windows-1252') as f:
        decode_text = f.read()

    raw_csv = decode_text.split('\n')  

    yield raw_csv

    raw_csv = None


@pytest.fixture
def comment_column_names_data_from_data_with_one_row():

    # From data_to_test_one_row.csv

    comment_header = ["#,,,,,,,,,,,,,,,,,,",
        "#Comment,,,,,,,,,,,,,,,,,,",
        "#,,,,,,,,,,,,,,,,,,",
        "#File Name,Zone,FIL:START TIME YYYY/MM/DD, HH:MM,LOC:EVENT_NUMBER,LOC:LATITUDE,LOC:LONGITUDE,LOC:STATION,INS:LOCATION,Pressure:CTD [dbar],Temperature:CTD [deg_C_(ITS90)],Salinity:CTD [PSS-78],Sigma-t:CTD [kg/m^3],Transmissivity:CTD [*/m],Oxygen:Dissolved:CTD:Volume [ml/l],Oxygen:Dissolved:CTD:Mass [µmol/kg],Fluorescence:CTD:Seapoint [mg/m^3],Fluorescence:CTD:Wetlabs [mg/m^3],PAR:CTD [µE/m^2/sec]",
        "#,,,,,,,,,,,,,,,,,,"]

    column_names = ['File Name', 'Zone', 'FIL:START TIME YYYY/MM/DD', ' HH:MM', 'LOC:EVENT_NUMBER', 'LOC:LATITUDE', 'LOC:LONGITUDE', 'LOC:STATION', 'INS:LOCATION', 'Pressure:CTD [dbar]', 'Temperature:CTD [deg_C_(ITS90)]', 'Salinity:CTD [PSS-78]', 'Sigma-t:CTD [kg/m^3]', 'Transmissivity:CTD [*/m]', 'Oxygen:Dissolved:CTD:Volume [ml/l]', 'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'Fluorescence:CTD:Seapoint [mg/m^3]', 'Fluorescence:CTD:Wetlabs [mg/m^3]', 'PAR:CTD [µE/m^2/sec]']

    # Expect list of lists
    data = [['2017-01-0017.ctd', 'UTC', '07-02-17', ' 16:40', '17', '48.6485', '-126.667', 'P4', 'Mid-ship', '2', '9.0615', '32.3599', '25.0678', '', '6.43', '280', '0.714', '', '64'], ['  ', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''], ['']]

    return comment_header, column_names, data


@pytest.fixture
def df_data_with_one_row(comment_column_names_data_from_data_with_one_row):

    comment_header, column_names, data = comment_column_names_data_from_data_with_one_row

    df_data_with_one_row = pd.DataFrame(data, columns=column_names)

    return df_data_with_one_row









