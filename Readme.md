# Overview


## Purpose of program

This program takes raw data from cruises listed at https://www.waterproperties.ca/linep/cruises.php and processes the data into the Exchange format. What I call raw data is the original data as is before any transformation to Exchange format.

The code takes a concatenated file composed of individual ctd files, identified by event numbers and station id, and converts each station to Exchange format with cast id, coming from incrementing event number, and station number.

I have mapped parameter names in individual files to Exchange parameters and converted date format for this grouping to Exchange format for cruises 2007-2018. Due to parameter names possibly changing and any date formats changing, I can't guarantee this program will work for other years. 

## Location and type of data processed

Cruise pages containing data are found from the page
```
https://www.waterproperties.ca/linep/cruises.php
```

The cruise page url is of the format
```
https://www.waterproperties.ca/linep/<cruise id>/index.php
```

As an example: 
There is a link to the cruise page given as '2007-01' where 2007 represents the year the data was taken and 01 represents a cruise id. Canadian data taken in 2007 with cruise id 01 and data from 7 - 26 February 2007 is found at the cruise page: https://www.waterproperties.ca/linep/2007-01/index.php. 

On a cruise page, there is a "Table of stations and sampling done" listing columns: Event, Station, Position, Date (UTC), Time (UTC), and Sampling. The sampling column indicates if the data is ctd or not. The links to these data sets are referred to as raw individual files. There is also a concatenated file containing all the raw individual files. It is found on the cruise page in the section labeled "Download data for the whole cruise". There, specifically CTD data can be selected and multiple casts are included in one file. 

In this program, I used an alternate method of getting the data. A listing of ctd files exists for each cruise. The concatenated file of individual files is found at 'https://www.waterproperties.ca/linep/<year>-<cruise_id>/donneesctddata/<year>-<cruise_id>-ctd-cruise.csv'. Which for an example data set would be https://www.waterproperties.ca/linep/2007-01/donneesctddata/2007-01-ctd-cruise.csv. The link https://www.waterproperties.ca/linep/2017-01/donneesctddata/ lists links to multiple ctd files including the individual ctd files. 


# Data Processing Steps

The concatenated form of the data is used for processing, which includes all stations for the cruise. The data is read into a Pandas dataframe. Line P stations specifically are selected to be converted to Exchange format and other stations are excluded. Line P stations are stations starting with 'P'. Pressure was imported as a float to sort on and all other columns were imported as strings.

Line P cruise IDs were mapped to expocodes using the start date and ship name found in the cruise report.

## Metadata

To extract the metadata used in the Exchange format, the following columns were used. 

FIL:START TIME YYYY/MM/DD
 HH:MM
LOC:LATITUDE
LOC:LONGITUDE
LOC:STATION
LOC:EVENT_NUMBER
LOC:STATION

The Zone column was not used since all date/times are UTC.

The metadata columns used in the Exchange file are as follows.

NUMBER_HEADERS
EXPOCODE
STNBR
CASTNO
DATE
TIME
LATITUDE
LONGITUDE

NUMBER_HEADERS is the number of meta data variables plus one for the NUMBER_HEADERS line. The expocode was from the mapped cruise id. LOC:STATION became STNBR and LOC:EVENT_NUMBER was used to create CASTNO. DATE and TIME were extracted from FIL:START TIME YYYY/MM/DD and HH:MM.  

## Parameters

Only a subset of the data was saved to the Exchange file and columns were renamed. The following columns are saved if they exist in the file:

CTDPRS, CTDTMP, CTDSAL, CTDOXY, CTDBEAMCP, CTDFLUOR, CTDFLUOR_TSG

The following were not included:
Oxygen:Dissolved:CTD:Volume [ml/l] 
Sigma-t:CTD [kg/m^3] and Sigma-t:CTD {kg/m^3]

There was a comments column for the 2010 cruise with id 01, and this was skipped.

Some data files call CTDSAL, CTDXMISS, and CTDFLUOR with different names and units, so account for this when importing columns. In the raw concatenated file, the units are included in the column header name. These are split out to create the Exchange file.
                           
  'whpname' : 'CTDPRS' , 'longname':'Pressure:CTD [dbar]', 'units' : 'DBAR'

  'whpname' : 'CTDTMP' , 'longname':'Temperature:CTD [deg_C_(ITS90)]', 'units' : 'ITS-90'
    
  'whpname' : 'CTDSAL' , 'longname':'Salinity:CTD [PSS-78]', 'units' : 'PSS-78'
    
  'whpname' : 'CTDSAL' , 'longname':'Salinity:Practical:CTD [PSS-78]', 'units' : 'PSS-78'
    
  'whpname' : 'CTDOXY' , 'longname':'Oxygen:Dissolved:CTD:Mass [µmol/kg]', 'units' : 'UMOL/KG'   
    
  'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [*/m]', 'units' : '/METER'
    
  'whpname' : 'CTDBEAMCP' , 'longname':'Transmissivity:CTD [%/m]', 'units' : '/METER'
    
  'whpname' : 'CTDFLUOR' , 'longname':'Fluorescence:CTD:Seapoint [mg/m^3]', 'units' : 'MG/M^3'   
    
  'whpname' : 'CTDFLUOR', 'longname': 'Fluorescence:CTD:Seapoint', 'units' : 'MG/M^3'
    
  'whpname' : 'CTDFLUOR', 'longname': 'Fluorescence:CTD [mg/m^3]', 'units' : 'MG/M^3'
    
  'whpname' : 'CTDFLUOR_TSG' , 'longname':'Fluorescence:CTD:Wetlabs [mg/m^3]', 'units' : 'MG/M^3'
    
  'whpname' : 'PAR' , 'longname':'PAR:CTD [µE/m^2/sec]', 'units' : 'UE/
    m^2/sec'

## Add flags

Add WHP flag columns with values for each data column. First, all flags are initially set to a value of 2. If there is no value in the data cell, fill with -999 and change flag to be 9. If a value is present in the data cell and equal to -99, change flag to be 5. Then change the -99 value to -999 which is the Exchange fill value for non-data values.

Flag = 5 represents 'Not reported'
Flag = 9 represents 'Not sampled'


## Map event number to cast number.

Each station number has multiple event numbers representing when data was taken. The data are grouped by event number and station name and then regrouped as cast number and station number. Cast number is determined from the increasing event number for each station. So map event numbers to a sequential cast number starting at 1 for each station.

As an example

station   Event #    Cast #
P20       event 18   1
P20       event 19   2
P20       event 26   3

## Add headers

Include header lines from individual raw file as comments in exchange ctd files. Each individual raw file has an event number so each cast number uses that corresponding header.

## Saving Exchange data

Data are saved by cast number and station number into a folder named as the ExpoCode and station number which is then zipped for each station number. 

  Rename files to Exchange format of  <EXPOCODE>_<P_STATION>_<CASTNO>_ct1

If stnbr has a slash in it, for file name, I replaced / with -

P1/B8 which I converted to P1-B8 for filename. In exchange file,
STNBR is P1/B8

  Save files in folder with name <EXPOCODE>_<P_STATION> and then zip this folder.


# Station number variations

Using only stations starting with P in data set representing Line P data.

Formats include

P# (P followed by a number)

P stations with non numeric characters in the name in the concatenated ctd file for whole cruise

  PIE93, PA-001 to PA-011, P1/B8, P14-P15, P24-P25, P20.5, P15.7, PAR


  PIE93 in 2007-13   (https://www.waterproperties.ca/linep/2007-13/index.php)

  PA-001 in cruise 2007-13  (https://www.waterproperties.ca/linep/2007-13/index.php)
  PA-011 in 2018-001 (https://www.waterproperties.ca/linep/2018-001/index.php)

  expect PA-003 but written as PA003, see 2009-09
  PA004 and PA-004 both used, see 2010-13 and 2010-14

  P15.7 in 2010-14  (https://www.waterproperties.ca/linep/2010-14/index.php) 

  P20.5 in 2011-01 (https://www.waterproperties.ca/linep/2011-01/index.php)  

  P14-P15 in cruise 2014-19  (Not listed as that on https://www.waterproperties.ca/linep/2014-19/index.php but listed as that in the ctd file. Listed as blank on the website.)

  P24-P25 in cruise 2014-19  (Not listed as that on https://www.waterproperties.ca/linep/2014-19/index.php but listed as that in the ctd file. Listed as blank on the website.)
   
  P1/B8 in 2015-09  (https://www.waterproperties.ca/linep/2015-09/index.php)

  PAR in 2015-10  (https://www.waterproperties.ca/linep/2015-10/index.php)


From Marie Robert:

PIE is a WOCE line that we sampled back in 1999.  In 2007 we resampled one of the stations on that line.
The PA-0xx stations are where the NOAA moorings are deployed.

Most of the others are CTD casts that we did to get PAR data for productivity casts the next morning.  So they were done at a specific time (~local noon) as opposed to a specific location.

P1/B8 are two station names for the same location but depending on which program goes out.  Since at the end of that cruise we visited 2 lines from the other program we gave both names to the station.


If file name has PA-006. More than 5 characters long to use in output
ctd filename.  So take out the hyphen in the name.


# Date and Time Patterns

Look at format of data sets and find out date and time 
formats and which need to be converted to exchange format.

Dates use both / and - in dates. File says dates of form YYYY/MM/DD but
really of form DD-MM-YY or DD/MM/YYYY. Will be converted to exchange YYYYMMDD

For FIL:START TIME YYYY/MM/DD, use two different versions for date

2007-01
17/02/2007 is of format DD/MM/YYYY

2012-13
15-08-12 is of format DD-MM-YY


Time has : in value, HH:MM, and will be converted to exchange format HHMM


# Exceptions and errors in original data

## Data pattern exceptions

### Dates
Dates are different than column reports. Sometimes / separated and others - separated. 

### Parameters
Files had different column names and units for same exchange column

### File names
If station number has slash in it, replace name with a dash

### Cruise date
For 2009 cruise with identifier 03, the menu reports dates from Jan 28 to Feb 8, 2009, but the cruise report has dates Jan 27 to Feb 10, 2009. I created expocode using cruise report dates


## Errors listing event number in concatenated file

Error for expocode 18DD20090606 with cruise year 2009 and identifier 09.
For P4, concatenated file says event 14 but table on website says should be event 15.

Error for expocode 18DD20090606 with cruise year 2009 and identifier 09.
For P19, concatenated file says event 51 but table on website says should be event 52.

This didn't affect increasing event number translated to cast number, but it does affect looking for the individual file so the header can be extracted. So I added code to search for these exceptions and use the proper files listed in the table.


## Incorrect column names for one concatenated file

Columns are wrong for for the concatenated file for cruise year 2009 and identifier 03.

All have an extra station number column with a header name of pressure. I then had edit the file to shift column header names over by one for each column.


Edit by hand.

For 10 headers there are 11 data columns.

Station Pressure Temperature Salinity Sigma-t Transmissivity  Oxygen:Dissolved:CTD:Volume  Oxygen:Dissolved:CTD:Mass Fluorescence PAR:CTD 
                    
SIO3  SIO3  2.3 6.9224  28.2974 22.1793 33.5  5.54  241.9 2.123 84.5


So import into Excel, remove extra column and make sure headers are for correct columns by shifting over by 1.

Place in adjusted_data folder to call program using that file.

I did not replace the raw concatenated file uploaded to CCHDO with corrected file of removing extra columns


## Selected files showing the column name variations

  2007-13
  Pressure:CTD [dbar],
  Temperature:CTD [deg_C_(ITS90)],
  Salinity:CTD [PSS-78],
  Transmissivity:CTD [*/m],
  Oxygen:Dissolved:CTD:Mass [µmol/kg]
  Fluorescence:CTD:Seapoint [mg/m^3],
  PAR:CTD [µE/m^2/sec]


  2007-15
  Pressure:CTD [dbar],
  Temperature:CTD [deg_C_(ITS90)],
  Salinity:Practical:CTD [PSS-78],
  Transmissivity:CTD [*/m],
  Oxygen:Dissolved:CTD:Mass [µmol/kg]
  Fluorescence:CTD [mg/m^3],
  PAR:CTD [µE/m^2/sec]


  2013-17
  Pressure:CTD [dbar],
  Temperature:CTD [deg_C_(ITS90)],
  Salinity:CTD [PSS-78],
  Transmissivity:CTD [%/m],
  Oxygen:Dissolved:CTD:Mass [µmol/kg]
  Fluorescence:CTD:Seapoint [mg/m^3],
  PAR:CTD [µE/m^2/sec]


  2015-01
  Pressure:CTD [dbar],
  Temperature:CTD [deg_C_(ITS90)],
  Salinity:CTD [PSS-78],
  Transmissivity:CTD [*/m],
  Oxygen:Dissolved:CTD:Mass [µmol/kg]
  Fluorescence:CTD:Seapoint,
  PAR:CTD [µE/m^2/sec]


  2017-01
  Pressure:CTD [dbar],
  Temperature:CTD [deg_C_(ITS90)],
  Salinity:CTD [PSS-78],
  Transmissivity:CTD [*/m],
  Oxygen:Dissolved:CTD:Mass [µmol/kg]
  Fluorescence:CTD:Seapoint [mg/m^3],
  Fluorescence:CTD:Wetlabs [mg/m^3],
  PAR:CTD [µE/m^2/sec]


# File Naming

Rename files to Exchange format

  <EXPOCODE>_<P_STATION>_<CASTNO>_ct1 for file

  and folder <EXPOCODE>_<P_STATION> for folder

If file name has PA-006. More than 5 characters long to use in output
ctd filename.  So take out the hyphen in the name.

If stnbr has a slash in it, for file name, I replaced / with -

e.g. expocode 18DD20150607

P1/B8 which I converted to P1-B8 for filename. In exchange file,
STNBR is P1/B8


# Setup and running the code

## Setup environment

Using python 3.7.5

1) To Install as a package
pip3 install convertLinePtoExchange

2) To use in an environment
python -m virtualenv env


## Packages used

Main packages used are numpy and pandas


## Config file

A config file is required to identify if integration testing to be run or run main program to create files. An output folder name is required and cruise list to map canadian line p cruise id to expocode. Config file CRUISE_LIST defines which raw files are processed. Match cruise id to expocode id. Use cruise report dates to create expocode want to upload data to.

Config file config.py must be in same folder 

Make sure TESTING is set to False


## Running the code

Activate the environment with the command 'source env/bin/activate' and run the following at the command line,

python3 convert_line_p_to_exchange.py


# To test the code

## Unit Tests
Set environment variable: PYTHON=.

Change into main folder containing tests folder. Run 
'''
python -m pytest
'''
from the command line. 


## Integration Tests (Fix documentation of these)

### Data output testing

1) file: data_to_test_castno.csv
   Testing: 
   Check castno created from event numbers and multiple line P ship 
   identifiers and one non line P ship identifier.
   Check it is skipping non line P stations.

2) file: data_to_test_castno_one_pline.csv
   Testing: 
   Check castno created from event numbers and one line P ship identifier
   and one non line P ship identifier.
   Check it is skipping non line P stations.

3) file: data_to_test_column_names1.csv
   Testing: 
   Unique pattern of line P parameter names chosen.
   Check column names converted to Exchange names.
   
4) file: data_to_test_column_names2.csv
   Testing: 
   Unique pattern of line P parameter names chosen.
   Check column names converted to Exchange names.

5) file: data_to_test_column_names3.csv
   Testing: 
   Unique pattern of line P parameter names chosen.
   Check column names converted to Exchange names.

6) file: data_to_test_column_names4.csv
   Testing: 
   Unique pattern of line P parameter names chosen.
   Check column names converted to Exchange names.

7) file: data_to_test_column_names5.csv
   Testing: 
   Unique pattern of line P parameter names chosen.
   Check column names converted to Exchange names.

8) file: data_to_test_date_format_w_dash.csv
   Testing: 
   Check dates with format with a dash: DD-MM-YY converted
   to Exchange format of YYYY-MM-DD

9) file: data_to_test_date_format_w_slash.csv
   Testing: 
   Check dates with format with a slash: DD/MM/YYYY converted
   to Exchange format of YYYY-MM-DD

10) file: data_to_test_fill_999.csv
   Testing: 
   Version with single line P identifier
   Check -99 line P data fill and empty entries converted to
   Exchange format fill of -999 and that -99 entries have
   a flag of 5 and empty entries have a flag of 9

11) file: data_to_test_fill_999_2.csv
   Testing: 
   Another version with multiple line P ship identifiers
   Check -99 line P data fill and empty entries converted to
   Exchange format fill of -999 and that -99 entries have
   a flag of 5 and empty entries have a flag of 9   

12) file: data_to_test_fill_999_3.csv
   Testing: 
   Another version with single line with single line P ship identifier.
   Check -99 line P data fill and empty entries converted to
   Exchange format fill of -999 and that -99 entries have
   a flag of 5 and empty entries have a flag of 9

13) file: data_to_test_flag_values.csv
   Testing: 
   Check that flags are value 5 for -99 fills and 
   flags are value 9 for empty entries (fill -999)

14) file: data_to_test_formatting.csv
   Testing: 
   Checking format spacing lines up same as original file.
   Preserves spacing between values when extra flag columns added

15) file: data_to_test_one_row.csv
   Testing: Check one line with line P ship identifier,
   empty values, and set of unique parameter names


16) file: data_to_test_stnbr_with_dash.csv
   Testing: 
   Check file name of stnbr with a slash in names is converted
   to replace slash with a dash in file name.


### To run integration tests
Change TESTING variable in config.py to True. 

Run
'''
python tests/integration_testing.py
'''




