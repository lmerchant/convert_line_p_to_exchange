class Config:

    TESTING = False

    # Top folder to save data folders in
    OUTPUT_DATA_FOLDER = './exchange_line_p_data'

    # Cruise list of format
    # CRUISE_LIST = [('year', 'cruise id', 'expocode')]
    # e.g.
    # CRUISE_LIST = [
    #     ('2018', '040', '18DD20180911'),
    # ]
    #

    # Cruises to update expocodes
    # June 15, 2021
    # see email from Bob Key May 3, 2021
    # In working up cruises for inclusion in GLODAP update, Nico Lange and Alex discovered the following 3 expocode errors. Nico confirmed the corrections from the Line P web site and further communication with Ana Franco who has assumed responsibility for this line. The correct codes are listed with the sailing date (old) indicated.

    # 18DD20100202(03)
    # 18DD20100605(06)
    # 18DD20140210(12)
    CRUISE_LIST = [
        ('2010', '01', '18DD20100203'),
        ('2010', '13', '18DD20100606'),
        ('2014', '01', '18DD20140212'),
    ]

    # Former Cruise list
    # CRUISE_LIST = [
    #     ('2020', '001', '18DD20200207'),
    #     ('2019', '001', '18DD20190205'),
    #     ('2019', '006', '18DD20190602'),
    #     ('2019', '008', '18DD20190813'),
    #     ('2018', '040', '18DD20180911'),
    #     ('2018', '001', '18LU20180218'),
    #     ('2018', '026', '18DD20180605'),
    #     ('2017', '001', '18DD20170205'),
    #     ('2017', '006', '18DD20170604'),
    #     ('2017', '008', '18DD20170815'),
    #     ('2016', '001', '18DD20160208'),
    #     ('2016', '006', '18DD20160605'),
    #     ('2016', '008', '18DD20160816'),
    #     ('2015', '001', '18DD20150210'),
    #     ('2015', '009', '18DD20150607'),
    #     ('2015', '010', '18DD20150818'),
    #     ('2014', '01', '18DD20140210'),
    #     ('2014', '18', '18DD20140608'),
    #     ('2014', '019', '18DD20140819'),
    #     ('2013', '01', '18DD20130205'),
    #     ('2013', '17', '18DD20130607'),
    #     ('2013', '18', '18DD20130820'),
    #     ('2012', '01', '18DD20120206'),
    #     ('2012', '12', '18DD20120522'),
    #     ('2012', '13', '18DD20120814'),
    #     ('2011', '01', '18DD20110208'),
    #     ('2011', '26', '18DD20110603'),
    #     ('2011', '27', '18DD20110816'),
    #     ('2010', '01', '18DD20100202'),
    #     ('2010', '13', '18DD20100605'),
    #     ('2010', '14', '18DD20100817'),
    #     ('2009', '03', '18DD20090127'),
    #     ('2009', '09', '18DD20090606'),
    #     ('2009', '10', '18DD20090818'),
    #     ('2008', '01', '18DD20080129'),
    #     ('2008', '26', '18DD20080528'),
    #     ('2008', '27', '18DD20080812'),
    #     ('2007', '01', '18DD20070207'),
    #     ('2007', '13', '18DD20070530'),
    #     ('2007', '15', '18DD20070814')
    # ]
