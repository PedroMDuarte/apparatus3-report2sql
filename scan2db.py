

import MySQLdb as mdb

import sys
import os
import glob
import time
import argparse
from configobj import ConfigObj
from sortedWalk import sortedWalk  
import datetime
absstart = datetime.datetime( 1903, 12, 31, 18, 00, 00 )

def db_insert_report( cur, created, lastmod, shot ):
    cur.execute("SELECT * FROM reports WHERE \
                 created_date = '%s' AND \
                 shot = %d" % (created, shot ) ) 
    rows = cur.fetchall()
    if len(rows) == 1:  # Report is already in table
        return 0
    elif len(rows) == 0:  # Report is not in table, go ahead and insert 
        cur.execute("INSERT INTO reports \
                     (created_date, lastmod_date, shot) \
                     VALUES('%s','%s',%d)" %  (created, lastmod, shot))
        return 1
    else:
        print "Database is corrupted, the same report appears"
        print "more than once in the reports table"



def db_create_reports_table( cur ):
    """ The reports table is created if it does not exist."""
    cur.execute("SHOW TABLES LIKE 'reports'")
    rows = cur.fetchall()
    if len(rows) == 1:  # Table already exists in database
        pass
    elif len(rows) == 0:  # Table is not in database, go ahead and create it
        print "Creating table reports" 
        cur.execute("CREATE TABLE reports( \
                     created_date DATETIME, lastmod_date DATETIME, shot INT UNSIGNED,\
                     CONSTRAINT pk_id PRIMARY KEY (created_date, shot) )")
    else:
        print "Database is corrupted, more than one reports table exists"


def db_insert_section( cur, sec, report, created, shot ):
    """ In our database schema each report SECTION corresponds to a 
        database table """

    tables_created = 0
    keys_updated = 0
  
    # 1. Check if there is already a table for this SECTION 
    cur.execute("SHOW TABLES LIKE '%s'"%sec)
    rows = cur.fetchall()
    if len(rows) == 1:  # Table already exists in database
        pass
    elif len(rows) == 0:  # Table is not in database, go ahead and create it
        print "Creating table %s" % sec
        cur.execute("CREATE TABLE %s (  \
                     created_date DATETIME, \
                     shot INT UNSIGNED,\
                     keystr VARCHAR(25), valuestr VARCHAR(500), valuenum DOUBLE,\
                     CONSTRAINT pk_%s PRIMARY KEY (created_date, shot, keystr),\
                     CONSTRAINT fk_report_%s FOREIGN KEY (created_date, shot) \
                        REFERENCES reports ( created_date, shot ) \
                     )" % (sec,sec,sec) )
        tables_created += 1 
    else:
        print "Database is corrupted, a section table (%s)"%sec
        print "appears more than once"

    # 2. Iterate over the section keys and add them to the table
    for key in report[sec].keys():
        #cur.execute("SELECT * FROM %s WHERE \
        #             created_date = '%s' AND \
        #             shot = %d AND\
        #             keystr = '%s'" % (sec, created, shot, key ) ) 
        #rows = cur.fetchall()
        #if len(rows) == 1:  # key is already in table
        #    pass
        #elif len(rows) == 0:  # key is not in table, go ahead and insert

        if type(report[sec][key]) == type([]):
            valuestr = ','.join(report[sec][key]) 
        else:
            valuestr = str(report[sec][key])
        try:
            float( valuestr ) 
            cur.execute("INSERT INTO %s \
                     (created_date, shot, keystr, valuestr, valuenum) \
                     VALUES('%s', %d, '%s', '%s', %s)\
                     ON DUPLICATE KEY UPDATE valuestr = '%s', valuenum = %s\
                     " %  (sec, created,  shot, key, valuestr, valuestr, valuestr, valuestr))
            #cur.execute("INSERT INTO %s \
            #             (created_date, shot, keystr, valuestr, valuenum) \
            #             VALUES('%s', %d, '%s', '%s', %s)" %  (sec, created,  shot, key, valuestr, valuestr))
        except:
            cur.execute("INSERT INTO %s \
                     (created_date, shot, keystr, valuestr, valuenum) \
                     VALUES('%s', %d, '%s', '%s', %s)\
                     ON DUPLICATE KEY UPDATE valuestr = '%s', valuenum = %s\
                     " %  (sec, created,  shot, key, valuestr, 'NULL', valuestr, 'NULL'))
            #cur.execute("INSERT INTO %s \
            #             (created_date, shot, keystr, valuestr, valuenum) \
            #             VALUES('%s', %d, '%s', '%s', %s)" %  (sec, created,  shot, key, valuestr, 'NULL'))
        keys_updated += 1 

            
    return tables_created, keys_updated 

if __name__ == "__main__":

    parser = argparse.ArgumentParser(__file__)
    parser.add_argument('DIR', action="store", type=str, help='scan directory path')
    parser.add_argument('--sec', nargs='+', type=str, help="list of sections to update")
    
    args = parser.parse_args()

    # Before running this program create the user and password for
    # a database called app3datadb by doing:
    #
    # $  mysql -u -root -p
    # >  create database app3datadb;
    # >  grant all privileges on app3datadb.* to 'app3user'@'localhost' identified by 'xyz'; 
    # >  quit; 
    #
    # After the database is populated you can enter mysql to use it:
    # 
    # >  mysql -u app3user -p app3datadb  
    # 
    # You will be asked for the password, it is xyz.

    con = mdb.connect('localhost', 'app3user', 'xyz', 'app3datadb')

    with con:
        cur = con.cursor()
        #cur.execute("DROP TABLE IF EXISTS reports")
        db_create_reports_table( cur )

        #cur.execute("INSERT INTO Writers(Name) VALUES('Truman Capote')")
        nprocessed = 0
        reports_inserted = 0
        tables_created = 0 
        keys_updated = 0 

        for rootdir, dirs, files in sortedWalk( args.DIR ):
            print "...Looking in ",rootdir
 
            sys.stdout.flush() 
            for basename in files:
                if 'report' in basename:
                    try:
                        reportpath = os.path.join(rootdir, basename )
                        report = ConfigObj( reportpath ) 
                    except Exception as e: 
                        print e
                        print "Could not load report %s" % reportpath
                        continue
                   
                     
                    shot = int( basename.split('.')[0].split('report')[1] )
                    timefmt = '%Y-%m-%d %H:%M:%S'
                    lastmod = time.strftime(timefmt, time.gmtime(os.path.getmtime(reportpath)))
                    try:
                        abstime = float( report['SEQ']['abstime'])
                        date    =  absstart + datetime.timedelta( seconds = abstime)
                        created = date.strftime(timefmt)
                        
                    except Exception as e: 
                        created = time.strftime(timefmt, time.gmtime(os.path.getctime(reportpath)))

                    reports_inserted += db_insert_report( cur, created, lastmod, shot )

    
                    # Go over all the sections in the report
                    # inserting them into the database 
                    for sec in report.keys():
                        if args.sec != None:
                            if sec not in args.sec:
                                continue
                        (new_tables, new_keys) = db_insert_section( cur, sec, report, created, shot)
                        tables_created += new_tables
                        keys_updated += new_keys
                    
    
                    nprocessed += 1
                    if nprocessed % 100 == 0: 
                        print " =>",nprocessed,"reports processed, ", reports_inserted,"reports inserted to db, ",
                        print tables_created,"tables created, ", keys_updated,"keys_updated"

        keys_updated = 0 
        print "Totals:" 
        print " =>",nprocessed,"reports processed, ", reports_inserted,"reports inserted to db, ",
        print tables_created,"tables created, ", keys_updated,"keys_updated"
        
           
      

