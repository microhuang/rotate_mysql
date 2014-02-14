#!/usr/bin/env python
# -*- coding:utf-8 -*-

#mysql partition range table  --  less int


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
    if args[0] not in config.db_mapping.db_mapping:
        print "cann't find configure"
        return 2
    conn=MySQLdb.connect(host=config.db_mapping.db_mapping[args[0]]['host'],user=config.db_mapping.db_mapping[args[0]]['user'],passwd=config.db_mapping.db_mapping[args[0]]['passwd'],db=config.db_mapping.db_mapping[args[0]]['db'],port=config.db_mapping.db_mapping[args[0]]['port'])
    cur=conn.cursor()
    sql="show create table %s" % args[0]
    cur.execute(sql)
    tcreate=cur.fetchone()
    p=re.compile('[\s\S]*PARTITION (\w+) VALUES LESS THAN \((.*)\),[\s\S]*PARTITION (\w+) VALUES LESS THAN \((.*)\)',re.I)
    tre=p.match(tcreate[1]).groups()
    sql="ALTER TABLE %s DROP PARTITION %s;" % (args[0],tre[0])
    cur.execute(sql)
    sql="ALTER TABLE %s ADD PARTITION (PARTITION %s VALUES LESS THAN (%s))" % (args[0],tre[0],tre[3]-tre[1])
    cur.execute(sql)
    return 0

if __name__ == "__main__":
    sys.exit(main())
