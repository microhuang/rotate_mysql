#!/usr/bin/env python
# -*- coding:utf-8 -*-

#mysql merge table


import sys
import getopt
import re
import MySQLdb

import config.db_mapping


def main():
    opts,args=getopt.getopt(sys.argv[1:],"Y")
    if len(args)!=1:
        print "Uasge:\n\t%s [options] <base_table>\noptions: \n\t-Y --force" % sys.argv[0]
        return 1
    '''db_host='localhost'
    db_user='root'
    db_passwd=''
    db_db='test'
    db_port=3306
    print db_mapping.db_mapping[args[0]]['host']
    exit()'''
    if args[0] not in config.db_mapping.db_mapping:
        print "cann't find configure"
        return 2
    conn=MySQLdb.connect(host=config.db_mapping.db_mapping[args[0]]['host'],user=config.db_mapping.db_mapping[args[0]]['user'],passwd=config.db_mapping.db_mapping[args[0]]['passwd'],db=config.db_mapping.db_mapping[args[0]]['db'],port=config.db_mapping.db_mapping[args[0]]['port'])
    cur=conn.cursor()
    sql="show create table %s" % args[0]
    #print sql
    cur.execute(sql)
    tcreate=cur.fetchone()
    #print tcreate[1]
    p=re.compile('[\s\S]*INSERT_METHOD=(\w+) UNION=\((.*)\)',re.I)
    tre=p.match(tcreate[1]).groups()
    tinsert=tre[0]
    tunion=tre[1].split(',')
    ttruncate=''
    #print tinsert,tunion
    if tinsert=='FIRST':
        ttruncate=tunion[len(tunion)-1]
        del tunion[len(tunion)-1]
        tunion.insert(ttruncate,0)
    elif tinsert=='LAST':
        ttruncate=tunion[0]
        del tunion[0]
        tunion.append(ttruncate)
    else:
        print "Is't a merge table"
        return 3
    #print ttruncate
    if ('-Y','') not in opts:
        print "%s will be truncate[Y/n]" % ttruncate
        if raw_input()!='Y':
            print "has cancel!"
            return 4
    sql='truncate table %s' % ttruncate
    #print sql
    cur.execute(sql)
    sql='alter table %s union=(%s)' % (args[0],(','.join(tunion)))
    #print sql
    cur.execute(sql)
    return 0

if __name__ == "__main__":
    sys.exit(main())
