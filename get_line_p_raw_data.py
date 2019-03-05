"""
  Get Canadian Line P cruise reports


  For ('2018', '001', '18LU20180218'),

  Raw data at 
  https://www.waterproperties.ca/linep/2018-001/donneesctddata/2018-001-ctd-cruise.csv

  download and prepend expocode to 18LU20180218_<RAW_DATA_NAME>.pdf

"""


from urllib.request import urlopen

def build_url(year,month):
    url = 'https://www.waterproperties.ca/linep/' + year + '-' + month + '/donneesctddata/' + year + '-' + month + '-ctd-cruise.csv'
    return url    


def save_raw_data(url, expocode):

    # Get filename
    url_list = url.split('/')
    raw_file_name = url_list[-1]

    # download cruise report year-month-cruise_report.pdf 
    # reaname to expocode_<RAW FILE NAME>.pdf
    output_file = "./raw_canadian_data/{}_{}".format(expocode, raw_file_name)

    response = urlopen(url)
    file = open(output_file, 'wb')
    file.write(response.read())
    file.close()

    print("Completed " + expocode)
 

def main():

    # year, month, expocode

    cruise_list = [
        ('2018', '001', '18LU20180218'),
        ('2018', '026', '18DD20180605'),
        ('2017', '01', '18DD20170205'),
        ('2017', '06', '18DD20170604'),
        ('2017', '08', '18DD20170815'),
        ('2016', '01', '18DD20160208'),
        ('2016', '06', '18DD20160605'),
        ('2016', '08', '18DD20160816'),
        ('2015', '01', '18DD20150210'),
        ('2015', '09', '18DD20150607'),
        ('2015', '10', '18DD20150818'),
        ('2014', '01', '18DD20140210'),
        ('2014', '18', '18DD20140608'),
        ('2014', '19', '18DD20140819'),
        ('2013', '01', '18DD20130205'),
        ('2013', '17', '18DD20130607'),
        ('2013', '18', '18DD20130820'),
        ('2012', '01', '18DD20120206'),
        ('2012', '12', '18DD20120522'),
        ('2012', '13', '18DD20120814'),
        ('2011', '01', '18DD20110208'),
        ('2011', '26', '18DD20110603'),
        ('2011', '27', '18DD20110816'),
        ('2010', '01', '18DD20100202'),
        ('2010', '13', '18DD20100605'),
        ('2010', '14', '18DD20100817'),
        ('2009', '03', '18DD20090127'),
        ('2009', '09', '18DD20090606'),
        ('2009', '10', '18DD20090818'),
        ('2008', '01', '18DD20080129'),
        ('2008', '26', '18DD20080528'),
        ('2008', '27', '18DD20080812'),
        ('2007', '01', '18DD20070207'),
        ('2007', '13', '18DD20070530'),
        ('2007', '15', '18DD20070814')
    ]


    for cruise in cruise_list:

        # Get URL of cruise report
        # https://www.waterproperties.ca/
        # linep/2018-001/documents/2018-001_cruise_report.pdf
        url = build_url(cruise[0],cruise[1])

        save_raw_data(url, cruise[2])



if __name__ == '__main__':

    main()
