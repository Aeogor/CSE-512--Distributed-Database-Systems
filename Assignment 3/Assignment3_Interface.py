#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    cur = openconnection.cursor()
    cur.execute("Drop table if exists " + OutputTable + ";")
    cur.execute("Create table " + OutputTable + " (like " + InputTable + " including all);")
    cur.execute("Select Max (" + SortingColumnName + "), Min (" + SortingColumnName + ") from " + InputTable + ";")
    high,low = (cur.fetchone())
    r = (float(high) - float(low))/5.0
    threads = [0,0,0,0,0]
    for i in range(5):
        min = low + i * r
        max = low + i * r + r
        threads[i] = threading.Thread(target=_parallelSort, args=(InputTable, SortingColumnName, i, min, max, openconnection))
        threads[i].start()

    i = 0
    for thread in threads:
        thread.join()
        cur.execute("Insert into " + OutputTable + " select * from sortThread" + str(i) + ";")
        i = i + 1
    
    cur.close()
    openconnection.commit()


def _parallelSort (InputTable, SortingColumnName, i, min, max, openconnection):
    cur = openconnection.cursor()
    cur.execute("Drop table if exists sortThread" + str(i) + " ;")
    cur.execute("Create table sortThread" + str(i) + " (like " + InputTable + " including all);")
    if(i == 0):
        cur.execute("Insert into sortThread" + str(i) + " select * from " + InputTable + " where " + SortingColumnName + " >= " + str(min) + " and " + SortingColumnName + " <= " + str(max) + " order by " + SortingColumnName + " ASC;")
    else:
        cur.execute("Insert into sortThread" + str(i) + " select * from " + InputTable + " where " + SortingColumnName + " > " + str(min) + " and " + SortingColumnName + " <= " + str(max) + " order by " + SortingColumnName + " ASC;")
    return

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()
    cur.execute("Drop table if exists " + OutputTable + ";")
    cur.execute("Select Max (" + Table1JoinColumn + "), Min (" + Table1JoinColumn + ") from " + InputTable1 + ";")
    high1,low1 = (cur.fetchone())
    cur.execute("Select Max (" + Table2JoinColumn + "), Min (" + Table2JoinColumn + ") from " + InputTable2 + ";")
    high2,low2 = (cur.fetchone())

    high = 0
    low = 0

    if(high1 >= high2):
        high = high1
    else: 
        high = high2
    
    if(low1 <= low2):
        low = low1
    else: 
        low = low2

    R = (high - low)/5.0
    threads = [0,0,0,0,0]

    for i in range(5):
        min = low + i * R
        max = low + i * R + R

        threads[i] = threading.Thread(target=_parrllelJoin, args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, min, max, i, openconnection))
        threads[i].start()
    
    for thread in threads:
        thread.join()

    cur.execute("Create table " + OutputTable + " as select * from " + InputTable1 + " inner join " + InputTable2 + " on " + InputTable1 + "." + Table1JoinColumn + " = " + InputTable2 + "." + Table2JoinColumn + " where 1 = 2;")
    i = 0
    for i in range(5):
        cur.execute("Insert into " + OutputTable + " select * from outputT" + str(i) + ";")

    cur.close()
    openconnection.commit()

def _parrllelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, min, max, i, openconnection):
    cur = openconnection.cursor()
    cur.execute("Drop table if exists outputT" + str(i) + ";")
    cur.execute("Drop table if exists table1_" + str(i) + ";")
    cur.execute("Drop table if exists table2_" + str(i) + ";")
    cur.execute("Create table table1_" + str(i) + " (like " + InputTable1 + ");")
    cur.execute("Create table table2_" + str(i) + " (like " + InputTable2 + ");")
    

    if(i == 0):
        cur.execute("create table outputT" + str(i) + " as select * from " + InputTable1 + " inner join " + InputTable2 + " on " + InputTable1 + "." + Table1JoinColumn + " = " + InputTable2 + "." + Table2JoinColumn + " where 1 = 2;")
        cur.execute("insert into table1_" + str(i) + " select * from " + InputTable1 + " where " + Table1JoinColumn + " >= " + str(min) + " and " + Table1JoinColumn + " <= " + str(max) + ";")
        cur.execute("insert into table2_" + str(i) + " select * from " + InputTable2 + " where " + Table2JoinColumn + " >= " + str(min) + " and " + Table2JoinColumn + " <= " + str(max) + ";")
    else: 
        cur.execute("create table outputT" + str(i) + " as select * from " + InputTable1 + " inner join " + InputTable2 + " on " + InputTable1 + "." + Table1JoinColumn + " = " + InputTable2 + "." + Table2JoinColumn + " where 1 = 2;")
        cur.execute("insert into table1_" + str(i) + " select * from " + InputTable1 + " where " + Table1JoinColumn + " > " + str(min) + " and " + Table1JoinColumn + " <= " + str(max) + ";")
        cur.execute("insert into table2_" + str(i) + " select * from " + InputTable2 + " where " + Table2JoinColumn + " > " + str(min) + " and " + Table2JoinColumn + " <= " + str(max) + ";")

    cur.execute("insert into outputT" + str(i) + " select * from table1_" + str(i) + " inner join table2_" + str(i) + " on table1_" + str(i) + "." + Table1JoinColumn + " = table2_" + str(i) + "." + Table2JoinColumn + ";")

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

