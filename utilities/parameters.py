def get_meta_params():

    # Mapping between pline names and WHP names for renaming columns

    # Space before HH:MM is required since space is in the line p column name
    
    params =[
    {'whpname' : 'DATE' , 'longname':'FIL:START TIME YYYY/MM/DD', 'units' : ''},                       
    {'whpname' : 'TIME' , 'longname':' HH:MM', 'units' : ''},
    {'whpname' : 'EVENT' , 'longname' : 'LOC:EVENT_NUMBER', 'units' : ''},                              
    {'whpname' : 'STATION' , 'longname':'LOC:STATION', 'units' : ''},                                  
    {'whpname' : 'LATITUDE' , 'longname':'LOC:LATITUDE', 'units' : ''},                               
    {'whpname' : 'LONGITUDE' , 'longname':'LOC:LONGITUDE', 'units' : ''},                             
    ]     

    return params


def get_all_data_params():

    # List of all possible pline parameter names

    # All choices of mapping between pline names and WHP names for renaming columns

    params =[                             
    {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},                          
    {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
    {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
    {'whpname' : 'CTDSAL' , 'longname':'Salinity:Practical:CTD [PSS-78]', 'units' : 'PSS-78'},
    {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},   
    {'whpname' : 'CTDXMISS' , 'longname':'Transmissivity:CTD [*/m]', 'units' : '/METER'},
    {'whpname' : 'CTDXMISS' , 'longname':'Transmissivity:CTD [%/m]', 'units' : '/METER'},
    {'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'},   
    {'whpname' : 'CTDFLUOR', 'longname': 'Fluorescence:CTD:Seapoint', 'units' : 'MG/M^3'},
    {'whpname' : 'CTDFLUOR', 'longname': 'Fluorescence:CTD [mg/m^3]', 'units' : 'MG/M^3'},
    {'whpname' : 'CTDFLUOR_TSG' , 'longname':'Fluorescence:CTD:Wetlabs [mg/m^3]', 'units' : 'MG/M^3'},
    {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}     
    ]        

    return params


def get_data_params(df):

    # Get only pline parameter names in dataframe so can just rename these
    # If try to rename pline parameter name not in dataframe, will get an error

    column_params = []
    
    all_params = get_all_data_params()       

    for param in all_params:

        param_name = param['longname']

        if param_name in df.columns:
            column_params.append(param)

    return column_params


def get_data_units(data_params):

    data_units_dict = {}

    for param in data_params:
        data_units_dict[param['whpname']] = param['units']

    return data_units_dict        
