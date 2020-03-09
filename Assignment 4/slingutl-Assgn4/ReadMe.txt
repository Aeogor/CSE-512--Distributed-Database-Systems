Srinivas Lingutla
CSE 512 - ASsignment 4
1217124532

References: https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0


		Approach
	-------------------------


Mapper
--------

The equijoinMapper function extends the inbuilt mapper function. This mapper function, creates
a key value pair. This key value pair is based on the column number from the input. The key used
in this case is based on the value in the 2nd column. Similarly, the value is the entire line. 
As you can in the function, I am splitting the input file based on newlines and then splitting 
each line further on commas to retrieve the 2nd column. I use this value as key and the entire 
as column which I pass it on to the reducer function

An Example of this:

Using the following Input
R, 2, Don, Larson, Newark, 555-3221
S, 1, 33000, 10000, part1
S, 2, 18000, 2000, part1
S, 2, 20000, 1800, part1
R, 3, Sal, Maglite, Nutley, 555-6905
S, 3, 24000, 5000, part1
S, 4, 22000, 7000, part1
R, 4, Bob, Turley, Passaic, 555-8908

The mapper function will generate the following
(key - value) aka (2nd column : Entire Row)

2 -> <R, 2, Don, Larson, Newark, 555-3221>
     <S, 2, 18000, 2000, part1>
     <S, 2, 20000, 1800, part1>

1 -> <S, 1, 33000, 10000, part1>

3 -> <R, 3, Sal, Maglite, Nutley, 555-6905>
     <S, 3, 24000, 5000, part1>

4 -> <S, 4, 22000, 7000, part1>
     <R, 4, Bob, Turley, Passaic, 555-8908>

Reducer
---------

In the reducer function, I initially create two arraylists or tables for the key from the mapper
function. Once I generated the lists and added the values onto the tables. Then I traverse through the two tables and join the equivalent entries and write that to the output. 

Based on the above key-value pairs, these are the tables generated

Table1: R, 2, Don, Larson, Newark, 555-3221
Table2: S, 2, 20000, 1800, part1

Combined: R, 2, Don, Larson, Newark, 555-3221, S, 2, 20000, 1800, part1

Table1: R, 2, Don, Larson, Newark, 555-3221
Table2: S, 2, 18000, 2000, part1

Combined: R, 2, Don, Larson, Newark, 555-3221, S, 2, 18000, 2000, part1

Table1: R, 3, Sal, Maglite, Nutley, 555-6905
Table2: S, 3, 24000, 5000, part1

Combined: R, 3, Sal, Maglite, Nutley, 555-6905, S, 3, 24000, 5000, part1

Table1: S, 4, 22000, 7000, part1
Table2: R, 4, Bob, Turley, Passaic, 555-8908

Combined: S, 4, 22000, 7000, part1, R, 4, Bob, Turley, Passaic, 555-8908

This is the final output which is a combination of all the combined outputs generated in reducer
-----------------------------------------------------------------------------------------------
R, 2, Don, Larson, Newark, 555-3221, S, 2, 20000, 1800, part1
R, 2, Don, Larson, Newark, 555-3221, S, 2, 18000, 2000, part1
R, 3, Sal, Maglite, Nutley, 555-6905, S, 3, 24000, 5000, part1
S, 4, 22000, 7000, part1, R, 4, Bob, Turley, Passaic, 555-8908





