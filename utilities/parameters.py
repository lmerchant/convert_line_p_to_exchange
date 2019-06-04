def get_meta_params():

    """
    Create mapping between Line P file names and WHP names.
    Will use later to rename columns to WHP name.
    """

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

    """
    Create mapping of all possible Line P parameter names to WHP names
    for renaming columns. Also map units.
    Put list in order want columns to appear in exchange output file.

    Some data files call CTDSAL, CTDBEAMCP, and CTDFLUOR with different names and units 
    so account for this when mapping columns.
    """

    params =[                             
    {'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'},
    {'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'},
    {'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'},
    {'whpname' : 'CTDSAL' , 'longname':'Salinity:Practical:CTD [PSS-78]', 'units' : 'PSS-78'},
    {'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'},   
    {'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [*/m]', 'units' : '/METER'},
    {'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [%/m]', 'units' : '/METER'},
    {'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'},   
    {'whpname' : 'CTDFLUOR', 'longname': 'Fluorescence:CTD:Seapoint', 'units' : 'MG/M^3'},
    {'whpname' : 'CTDFLUOR', 'longname': 'Fluorescence:CTD [mg/m^3]', 'units' : 'MG/M^3'},
    {'whpname' : 'CTDFLUOR_TSG' , 'longname':'Fluorescence:CTD:Wetlabs [mg/m^3]', 'units' : 'MG/M^3'},
    {'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/m^2/sec'}     
    ]        

    return params


def get_data_params(df):

    """
    Get data parameter names in the file. Not all parameter (column) names exist in 
    each file so getting a subset from the parameter mapping dictionary. 
    Subset will be in order of those listed in the parameter mapping dictionary.

    Want subset of parameter names so know which columns to rename.
    If try to rename a parameter not in the dataframe, will get an error.
    """

    column_params = []
    
    all_params = get_all_data_params()       

    for param in all_params:

        param_name = param['longname']

        if param_name in df.columns:
            column_params.append(param)

    return column_params


def get_data_units(data_params):

    """Get mapping of WHP name and units."""

    data_units_dict = {}

    for param in data_params:
        data_units_dict[param['whpname']] = param['units']

    return data_units_dict        
