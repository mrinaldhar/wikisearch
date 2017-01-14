=================================================================

README :: Phase 1 :: Information Retrieval and Extraction project

						Mrinal Dhar
						201325118

=================================================================

The code folder contains two files:

	1. seq.py : This runs the indexer sequentially over all
		documents extracted from XML.

	2. parallel.py: This runs the indexer parallelly, 
		distributed over multiple processes according to your 
		computer's processing power. 

The default method used by the bash script is parallel.

The output produced by both files is the same.

The parallel processing file produces the result upto 50% faster 
on a Core i5 processor with 4 cores. 
(3 documents parsed at the same time)

This is expected to be improved further on a machine with more
cores. Example: On an 8 core machine, 7 documents will be 
processed at once.

=================================================================