# This script should update in the database any contents 
# of the analysis sections in the reports 
# analysis sections :  CPP, ANDOR1EIGEN, ANDOR2EIGEN, HHHEIGEN, ANALYSI

ASEC = ['CPP', 'ANDOR1EIGEN', 'ANDOR2EIGEN', 'MANTAEIGEN', 'HHHEIGEN']

import MySQLdb as mdb

import sys
import os
import glob
import time
import argparse
from configobj import ConfigObj
from sortedWalk import sortedWalk  
import datetime

def db_update_section( cur, sec, report, created, shot ):
    """ In our database schema each report SECTION corresponds to a 
        database table """

    tables_created = 0
    keys_inserted = 0
  
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
                     CONSTRAINT fk_report_%s FOREIGN KEY (created_date, shot) \
                        REFERENCES reports ( created_date, shot ) \
                     )" % (sec,sec) )
        tables_created += 1 
    else:
        print "Database is corrupted, a section table (%s)"%sec
        print "appears more than once"

    # 2. Iterate over the section keys and add them to the table
    for key in report[sec].keys():
        cur.execute("SELECT * FROM %s WHERE \
                     created_date = '%s' AND \
                     shot = %d AND\
                     keystr = '%s'" % (sec, created, shot, key ) ) 
        rows = cur.fetchall()
        if len(rows) == 1:  # key is already in table
            pass
        elif len(rows) == 0:  # key is not in table, go ahead and insert

        if type(report[sec][key]) == type([]):
            valuestr = ','.join(report[sec][key]) 
        else:
            valuestr = str(report[sec][key])
        try:
            float( valuestr ) 
            cur.execute("INSERT INTO %s \
                         (created_date, shot, keystr, valuestr, valuenum) \
                         VALUES('%s', %d, '%s', '%s', %s)\
                         ON DUPLICATE KEY UPDATE valuestr = '%s', valuenum = %s
                         " %  (sec, created,  shot, key, valuestr, valuestr, valuestr, valuestr))
        except:
            cur.execute("INSERT INTO %s \
                         (created_date, shot, keystr, valuestr, valuenum) \
                         VALUES('%s', %d, '%s', '%s', %s)" %  (sec, created,  shot, key, valuestr, 'NULL'))
        keys_inserted += 1 
            
    return tables_created, keys_inserted 

if __name__ == "__main__":

    parser = argparse.ArgumentParser(__file__)
    parser.add_argument('DIR', action="store", type=str, help='scan directory path')
    
    args = parser.parse_args()

    con = mdb.connect('localhost', 'app3user', 'xyz', 'app3datadb')

    with con:
        cur = con.cursor()

        for rootdir, dirs, files in sortedWalk( args.DIR ):
            print "...Looking in ",rootdir
            nprocessed = 0
            reports_inserted = 0
            tables_created = 0 
            keys_inserted = 0 
 
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
                    created = time.strftime(timefmt, time.gmtime(os.path.getctime(reportpath)))

                    # Go over all the sections in the report
                    # inserting them into the database 
                    for sec in report.keys():
                        if sec in ASEC:
                            (new_tables, new_keys) = db_update_section( cur, sec, report, created, shot)
                            tables_created += new_tables
                            keys_inserted += new_keys
                    
    
                    nprocessed += 1
                    if nprocessed % 50 == 0: 
                        print " =>",nprocessed,"reports processed, ", reports_inserted,"reports inserted to db, ",
                        print tables_created,"tables created, ", keys_inserted,"keys_inserted"

            print "Totals:" 
            print " =>",nprocessed,"reports processed, ", reports_inserted,"reports inserted to db, ",
        
           
      

