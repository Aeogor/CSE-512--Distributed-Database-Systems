# Srinivas Lingutla
# 1217124532 - slingutl
# CSE 512 - DDS

#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# ---------------------------------------------------------------------------------------------------------------------

#Implement a Python function LoadRatings() that takes a file system absolute path that contains the
#rating.dat file as input. LoadRatings() then loads the rating.dat content into a table (saved in
#PostgreSQL) named Ratings that has the following schema

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    #print("Executing Load Ratings from the filepath")
    
    current = openconnection.cursor()

    dropTable = "drop table if exists " + ratingstablename + ";"
    createTable = "create table if not exists " + ratingstablename + " (userid INT, movieid INT, rating float);"

    current.execute(dropTable)
    current.execute(createTable)

    ratings_file = open(ratingsfilepath, 'r')
    for record in ratings_file.readlines():
        cols = record.split('::')
        insertTable = "INSERT INTO " + ratingstablename + " (userid, movieid, rating) VALUES (" + cols[0] + ", " + cols[1] + ", " + cols[2] + ");"
        current.execute(insertTable)

    #print("Finished Loading the Ratings")

    openconnection.commit()
    current.close()
    
# ---------------------------------------------------------------------------------------------------------------------

#Implement a Python function Range_Partition() that takes as input: (1) the Ratings table stored in
#PostgreSQL and (2) an integer value N; that represents the number of partitions. Range_Partition()
#then generates N horizontal fragments of the Ratings table and store them in PostgreSQL. The
#algorithm should partition the ratings table based on N uniform ranges of the Rating attribute.

def rangepartition(ratingstablename, numberofpartitions, openconnection):
	current = openconnection.cursor()

	dropTable = "drop table if exists range_part"
    	createTable = "create table if not exists range_part"

    	for i in range(numberofpartitions):
        	current.execute(dropTable + str(i) + ";")

    	for i in range(numberofpartitions):
        	current.execute(createTable + str(i) + " (userid INT, movieid INT, rating float);" )

    	insertFirstPartition = """insert into range_part0 select 
        	* from """ + ratingstablename + " where rating >= 0 and rating <= " + str(5.0/numberofpartitions) + " ;"
	
	current.execute(insertFirstPartition)

    	for i in range(numberofpartitions):
      		
      		if i == 0:
       	  		continue
       	 	else:
             		insertRemainingPartitions = """insert into range_part""" + str(i) + """ select * 
                	from """ + ratingstablename + """ where rating > """ + str(i * 5.0 / numberofpartitions) + """
                	and rating <= """ + str((i + 1) * 5.0/numberofpartitions) + " ;"
            		
			current.execute(insertRemainingPartitions)
    	openconnection.commit()


	dropTotalPartitionsTable = "drop table if exists total_r_partitions;"
    	createTableTotalPartitions = "create table if not exists total_r_partitions (count INT);"
    	insertTableTotalPartitions = "insert into total_r_partitions values ( " + str(numberofpartitions) + " );"

    	current.execute(dropTotalPartitionsTable)
    	current.execute(createTableTotalPartitions)
    	current.execute(insertTableTotalPartitions) 

    	openconnection.commit()
    	current.close()

# ---------------------------------------------------------------------------------------------------------------------

#Implement a Python function RoundRobin_Partition() that takes as input: (1) the Ratings table
#stored in PostgreSQL and (2) an integer value N; that represents the number of partitions. The
#function then generates N horizontal fragments of the Ratings table and stores them in PostgreSQL.
#Thealgorithmshouldpartitiontheratings tableusingthe roundrobinpartitioningapproach(explained
#in class).

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    current = openconnection.cursor()


    dropMetaData = "drop table if exists meta_data_table;"
    createMetaData = """create table if not exists meta_data_table as 
                        select userid, movieid, rating, rownum from
                        (select userid, movieid, rating, ROW_NUMBER() OVER(order by userid) 
                         rownum from """ + ratingstablename + " ) a;"

    current.execute(dropMetaData)
    current.execute(createMetaData)

    for i in range(numberofpartitions):
        dropRoundRobinPartition = "drop table if exists rrobin_part" + str(i) + " ;"
        current.execute(dropRoundRobinPartition)

    mostRecentPartition = 0

    for i in range(numberofpartitions):
        j = i + 1
        createRoundRobinPartition = "create table if not exists rrobin_part" + str(i) + """
                                    as select userid, movieid, rating from meta_data_table 
                                    where (rownum - """ + str(j) + """ ) % 5 = 0;"""
        current.execute(createRoundRobinPartition)
        mostRecentPartition = i

    dropTotalPartitionsTable = "drop table if exists total_partitions;"
    createTableTotalPartitions = "create table if not exists total_partitions (count INT, recent INT);"
    insertTableTotalPartitions = "insert into total_partitions values ( " + str(numberofpartitions) + ", " +  str(mostRecentPartition) + " );"

    current.execute(dropTotalPartitionsTable)
    current.execute(createTableTotalPartitions)
    current.execute(insertTableTotalPartitions)  


    openconnection.commit()
    current.close()

#---------------------------------------------------------------------------------------------------------------------

#Implement a Python function RoundRobin_Insert() that takes as input: (1) Ratings table stored in
#PostgreSQL, (2) UserID, (3) ItemID, (4) Rating. RoundRobin_Insert() then inserts a new tuple to the
#Ratings table and the right fragment based on the round robinapproach.

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    current = openconnection.cursor()
    
    getTotalPartitions = "select count, recent from total_partitions;"
    insertRatingsTable = "insert into " + ratingstablename + " (userid, movieid, rating) values (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");"
    
    current.execute(insertRatingsTable)

    current.execute(getTotalPartitions)
    partitionCount = int(current.fetchone()[0])
    
    totalRecordsQuery = "select count(userid) from " + ratingstablename + ";"
    current.execute(totalRecordsQuery)

    insertIntoPartitionTable = "insert into rrobin_part" + str((current.fetchone()[0] - 1) % partitionCount) + " (userid, movieid, rating) values (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");"

    current.execute(insertIntoPartitionTable)

    openconnection.commit()
    current.close()

#---------------------------------------------------------------------------------------------------------------------

#Implement a Python function Range_Insert() that takes as input: (1) Ratings table stored in Post-
#greSQL (2) UserID, (3) ItemID, (4) Rating. Range_Insert() then inserts a new tuple to the Ratings
#table and the correct fragment (of the partitioned ratings table) based upon the Rating value.

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    current = openconnection.cursor()
    
    getTotalPartitions = "select count from total_r_partitions;"
    insertRatingsTable = "insert into " + ratingstablename + " (userid, movieid, rating) values (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");"

    current.execute(getTotalPartitions)
    partitionCount = current.fetchone()[0]
    
    current.execute(insertRatingsTable)

    if(rating <= 5.0/partitionCount and rating >=0):
        insertFirstPartition = "insert into range_part0  (userid, movieid, rating) values (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");"
        current.execute(insertFirstPartition)
    else: 
        for i in range(partitionCount):
            if (i == 0): 
                continue
            else :
                if (rating > i * 5.0 / partitionCount) and (rating <= (i + 1) * 5.0 / partitionCount) :
                    insertRow = "insert into range_part" + str(i) + " (userid, movieid, rating) values (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");"
                    current.execute(insertRow)

    
    openconnection.commit()
    current.close()



def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()
