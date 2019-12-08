import pandas as pd

from convert_line_p_data_to_exchange.utilities.headers import *


def test_create_column_headers():

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


    expected_header = ['CTDPRS,CTDPRS_FLAG_W,CTDTMP,CTDTMP_FLAG_W,CTDSAL,CTDSAL_FLAG_W,CTDOXY,CTDOXY_FLAG_W,CTDBEAMCP,CTDBEAMCP_FLAG_W,CTDFLUOR,CTDFLUOR_FLAG_W,CTDFLUOR_TSG,CTDFLUOR_TSG_FLAG_W,PAR,PAR_FLAG_W',
    'DBAR,,ITS-90,,PSS-78,,UMOL/KG,,/METER,,MG/M^3,,MG/M^3,,UE/m^2/sec,']

    real_header = create_column_headers(testing_data_params)

    assert real_header == expected_header



def test_create_metadata_header():

    column_names = ['File Name', 'Zone', 'DATE', 'TIME', 'EVENT', 'LATITUDE', 'LONGITUDE', 'EXPOCODE', 'STATION', 'CASTNO', 'INS:LOCATION', 'CTDPRS']

    data = [['2017-01-0017.ctd', 'UTC', '20170207', '1640', '18', '48.6485', '-126.667', '<expocode>', 'P4', 'P4_2', 'Mid-ship', '2'], ['2017-01-0017.ctd', 'UTC', '20170207', '1640', '18', '48.6485', '-126.667', '<expocode>', 'P4', 'P4_2', 'Mid-ship', '2']]

    testing_df = pd.DataFrame(data, columns = column_names)

    expected_header = ['NUMBER_HEADERS = 8', 'EXPOCODE = <expocode>','STNBR = P4','CASTNO = P4_2','DATE = 20170207','TIME = 1640','LATITUDE = 48.6485', 'LONGITUDE = -126.667'] 

    real_header = create_metadata_header(testing_df)

    assert real_header == expected_header



