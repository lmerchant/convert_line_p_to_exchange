import pytest
import pandas as pd

from convert_line_p_data_to_exchange.utilities.parameters import *



@pytest.fixture()
def df_column_name_sets():

    # Mapping of the original column names to WHP names. Sets were extracted from real files
    # to get all combinations of parameter long names that exist. 

    column_names = ['File Name','Pressure:CTD [dbar]','Temperature:CTD [deg_C_(ITS90)]','Salinity:CTD [PSS-78]','Sigma-t:CTD [kg/m^3]','Transmissivity:CTD [*/m]','Oxygen:Dissolved:CTD:Volume [ml/l]','Oxygen:Dissolved:CTD:Mass [µmol/kg]','Fluorescence:CTD:Seapoint [mg/m^3]','PAR:CTD [µE/m^2/sec]']
    data = [['2007-13-0003.ctd',3.2,'11.0119','31.4992','24.0747','18.4','6.98','304.4','6.05','']]
    testing_df_set1 = pd.DataFrame(data, columns=column_names)

    expected_params_set1 =[                             
    {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},
    {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
    {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
    {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},   
    {'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [*/m]', 'units' : '/METER'},
    {'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'},   
    {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}     
    ] 

    column_names = ['File Name','Pressure:CTD [dbar]','Temperature:CTD [deg_C_(ITS90)]','Salinity:Practical:CTD [PSS-78]','Sigma-t:CTD [kg/m^3]','Transmissivity:CTD [*/m]','Oxygen:Dissolved:CTD:Volume [ml/l]','Oxygen:Dissolved:CTD:Mass [µmol/kg]','Fluorescence:CTD [mg/m^3]','PAR:CTD [µE/m^2/sec]']
    data = [['2007-15-0003.ctd',4.0,'11.4455','31.1325','23.7134','33.8','5.24','228.7','2.242','0.2']]
    testing_df_set2 = pd.DataFrame(data, columns=column_names)

    expected_params_set2 =[                             
    {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},
    {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
    {'whpname' : 'CTDSAL' , 'longname':'Salinity:Practical:CTD [PSS-78]', 'units' : 'PSS-78'},
    {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},   
    {'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [*/m]', 'units' : '/METER'},
    {'whpname' : 'CTDFLUOR', 'longname': 'Fluorescence:CTD [mg/m^3]', 'units' : 'MG/M^3'},
    {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}     
    ] 


    column_names = ['File Name','Pressure:CTD [dbar]','Temperature:CTD [deg_C_(ITS90)]','Salinity:CTD [PSS-78]','Sigma-t:CTD [kg/m^3]','Transmissivity:CTD [%/m]','Oxygen:Dissolved:CTD:Volume [ml/l]','Oxygen:Dissolved:CTD:Mass [µmol/kg]','Fluorescence:CTD:Seapoint [mg/m^3]','PAR:CTD [µE/m^2/sec]']
    data = [['2013-17-0002.ctd',3.0,'12.0342','30.2748','22.9419','23.9','6.28','274.1','2.002','248.8']] 
    testing_df_set3 = pd.DataFrame(data, columns=column_names)

    expected_params_set3 =[                             
    {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},
    {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
    {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
    {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},   
    {'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [%/m]', 'units' : '/METER'},
    {'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'},   
    {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}     
    ] 


    column_names = ['File Name','Pressure:CTD [dbar]','Temperature:CTD [deg_C_(ITS90)]','Salinity:CTD [PSS-78]','Sigma-t:CTD [kg/m^3]','Transmissivity:CTD [*/m]','Oxygen:Dissolved:CTD:Volume [ml/l]','Oxygen:Dissolved:CTD:Mass [µmol/kg]','Fluorescence:CTD:Seapoint','PAR:CTD [µE/m^2/sec]']
    data = [['2015-01-0010.ctd',2.0,'11.1569','32.0634','24.4881','58.9','6.29','274.1','0.625','376.4']]
    testing_df_set4 = pd.DataFrame(data, columns=column_names)

    expected_params_set4 =[                             
    {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},
    {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
    {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
    {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},   
    {'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [*/m]', 'units' : '/METER'},
    {'whpname' : 'CTDFLUOR', 'longname': 'Fluorescence:CTD:Seapoint', 'units' : 'MG/M^3'},
    {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}     
    ] 


    column_names = ['File Name','Pressure:CTD [dbar]','Temperature:CTD [deg_C_(ITS90)]','Salinity:CTD [PSS-78]','Sigma-t:CTD [kg/m^3]','Transmissivity:CTD [*/m]','Oxygen:Dissolved:CTD:Volume [ml/l]','Oxygen:Dissolved:CTD:Mass [µmol/kg]','Fluorescence:CTD:Seapoint [mg/m^3]','Fluorescence:CTD:Wetlabs [mg/m^3]','PAR:CTD [µE/m^2/sec]']
    data = [['2017-01-0016.ctd',1.3,'9.1383','32.541','25.1974','','','','0.748','','0.1']]
    testing_df_set5 = pd.DataFrame(data, columns=column_names)

    expected_params_set5 =[                             
    {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},
    {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
    {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
    {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},   
    {'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [*/m]', 'units' : '/METER'},
    {'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'},   
    {'whpname' : 'CTDFLUOR_TSG' , 'longname':'Fluorescence:CTD:Wetlabs [mg/m^3]', 'units' : 'MG/M^3'},
    {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}     
    ] 


    testing_df_sets = (testing_df_set1, testing_df_set2, testing_df_set3, testing_df_set4, testing_df_set5)

    expected_param_sets = (expected_params_set1, expected_params_set2, expected_params_set3, expected_params_set4, expected_params_set5)

    return testing_df_sets, expected_param_sets


def test_get_data_params(df_column_name_sets):

    # For each variation of column names, confirm the 
    # mapping of their long names to WHP names

    testing_df_sets, expected_param_sets = df_column_name_sets

    #real_params = get_data_params(testing_df_sets[0])

    for expected_params, testing_df in zip(expected_param_sets, testing_df_sets):

        real_params = get_data_params(testing_df)

        assert real_params == expected_params


def test_get_data_units():

    data_params =[                             
    {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},
    {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
    {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
    {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},   
    {'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [*/m]', 'units' : '/METER'},
    {'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'},   
    {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}     
    ] 

    expected_data_units = {'CTDPRS': 'DBAR', 'CTDTMP': 'ITS-90', 'CTDSAL': 'PSS-78', 'CTDOXY': 'UMOL/KG', 'CTDBEAMCP': '/METER', 'CTDFLUOR': 'MG/M^3', 'PAR': 'UE/m^2/sec'} 


    real_data_units = get_data_units(data_params)

    assert real_data_units == expected_data_units

