=================================================================

README :: Phase 2 :: Information Retrieval and Extraction project

						Mrinal Dhar
						201325118

=================================================================

The code folder contains three files:

	1. search.py: This is the search process that also launches
		a web server at port 5005 giving you a web interface 
		for the search engine. 

	2. parallel.py: This runs the indexer parallelly, 
		distributed over multiple processes according to your 
		computer's processing power. 

	3. index.html: The interface for the web search engine. 

The parallel processing file produces the result upto 50% faster 
on a Core i5 processor with 4 cores. 
(3 documents parsed at the same time)

This is expected to be improved further on a machine with more
cores. Example: On an 8 core machine, 7 documents will be 
processed at once.

=================================================================