

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


if __name__ == "__main__":

    parser = argparse.ArgumentParser(__file__)
    parser.add_argument('DIR', action="store", type=str, help='scan directory path')
    parser.add_argument('--sec', nargs='+', type=str, help="list of sections to update")
    
    args = parser.parse_args()

    for rootdir, dirs, files in sortedWalk( args.DIR ):
        print "...Looking in ",rootdir
        nprocessed = 0
        reports_inserted = 0
        tables_created = 0 
        keys_updated = 0 
 
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
               
 
                print shot
                print created 
                print lastmod
                print abstime


