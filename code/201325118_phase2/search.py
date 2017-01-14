import aspell
import pickle 
from parallel import get_data, tokenize
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import SocketServer
import urllib 
import multiprocessing as mp
import sys
import operator
import json
import time
en_dict = aspell.Speller('lang', 'en')									# Initialize the english dictionary
pagemap = pickle.load(open(sys.argv[1]+'_pagemap'))						# Mapping for Page ID to Page title
index_file = open(sys.argv[1]).readlines()								# Open the main index file
cache = {}																# Maintain a cache of all terms that you interact with.

'''
Handler for the integrated web server interface
'''
class S(BaseHTTPRequestHandler):
	def _set_headers(self):
		self.send_response(200)
		self.send_header('Content-type', 'text/html')
		self.end_headers()


	'''
	Handles GET requests for the server
	'''
	def do_GET(self):
		start = time.time()
		self._set_headers()
		reqPath = self.path.split("?query=")
		if reqPath[0] == '/search':										# If the request is for a search process, read the query term.
			reqPath[1] = urllib.unquote(reqPath[1]).decode('utf8')		# For URL decoding
			origQuery = reqPath[1].split('+')
			query = tokenize(" ".join(reqPath[1].split('+')))			# Run the same tokenizer on the query that was run on index terms.
			altQuery = ""												# For the "Did you mean ...?" query response :P
			for x in xrange(0, len(origQuery)):
				word = origQuery[x]
				if word != '':
					altQuery += en_dict.suggest(word.lower())[0].lower()
				if x!=len(origQuery)-1:
					altQuery += " "
			postings = []												# The main postings list that will be combined for final results.
			for word in query:
				if not cache.has_key(word): 							# Check if word is in cache, making query faster
					search_results = search_query(word, index_file, 0, len(index_file))
					cache[word] = search_results[1]
				else:
					search_results = [word, cache[word]]
				postings.append(search_results[1])

			merged_postings = combine_postings(postings, False)
			results = {}												# Final JSON object that will be returned
			results["suggest"] = altQuery
			results["data"] = {}
			if len(merged_postings.keys())>0:
				count = 0	
				for key in merged_postings:
					smallCount = 0
					results["data"][key] = []	
					topList = sorted(merged_postings[key].items(), key=operator.itemgetter(1), reverse=True)	# Sort by tf
					for docID, tf in topList:
						count += 1
						if smallCount < 15:								# Limit to 15 docs
							smallCount += 1
							results["data"][key].append(pagemap[docID])
				results["number"] = count	
			else:
				results["number"] = 0

			end = time.time()
			results["time"] = round(end - start, 5)
			self.wfile.write(json.dumps(results))
		elif reqPath[0] == '/':											# Request for homepage
			self.wfile.write(open('index.html').read())	
		else:															# Request for all other files
			self.wfile.write(open(reqPath[0][1:]).read())

	def do_HEAD(self):
		self._set_headers()
		
	def do_POST(self):
		# Doesn't do anything with posted data
		self._set_headers()
		self.wfile.write("<html><body><h1>POST requests are not needed for this service!</h1></body></html>")
		
'''
Starts the HTTP server
'''
def run_server(server_class=HTTPServer, handler_class=S, port=5005):
	server_address = ('', port)
	httpd = server_class(server_address, handler_class)
	print 'Starting httpd...'
	httpd.serve_forever()


'''
Uses Binary Search in the index file to 
locate query term
'''
def search_query(query, index, i, j):
	mid = (i+j)/2
	if (i > j):
		return "No matches found."
	if mid < len(index):
		currTerm = get_data(index[mid])
		cache[currTerm[0]] = currTerm[1]
		if (query < currTerm[0]):
			return search_query(query, index, i, mid-1)
		elif (query > currTerm[0]):
			return search_query(query, index, mid+1, j)
		elif (query == currTerm[0]):
			return currTerm
	else:
		return "No matches found."
	

'''
Combines postings in the search results 
according to tf - idf value.
'''
def combine_postings(postings, limit=True):
	results = {}
	for posting in postings:
		if isinstance(posting, dict):
			for key in posting.keys():
				count = 0
				for docID, tf in posting[key]:
					count += 1
					if limit and count > 6:
						break
					if key in results.keys():
						if docID in results[key]:
							results[key][docID] += tf
						else:
							results[key][docID] = tf
					else:
						results[key] = { docID: tf }
	return results


'''
Main runner process
'''
def main():
	c = mp.Process(target=run_server)				# HTTP server process
	c.start()

	while 1:										# Allow searching indefinitely.
		origQuery = raw_input("Enter a search query: ")
		query = origQuery.split(':')
		if len(query) > 1:
			postings = []
			for x in xrange(0, len(query)):
				each = query[x]
				count = query.index(each)
				if count == len(query)-1:			
					newquery = tokenize(each)
				else:
					newquery = tokenize(each[0:-1])		# Complicated handler for "b: india" type queries. Maybe simplify this in next version.

				for word in newquery:
					print 
					print "Searching for "+word+" ..."
					print
					if not cache.has_key(word):
						search_results = search_query(word, index_file, 0, len(index_file))
						cache[word] = search_results[1]
					else:
						search_results = [word, cache[word]]
					if search_results[1].has_key(query[x-1][-1]):
						postings.append({query[x-1][-1]: search_results[1][query[x-1][-1]]})	# Save only those results which are needed.	eg. b: or c: 
		else:
			postings = []
			newquery = tokenize(origQuery)
			for word in newquery:
					print 
					print "Searching for "+word+" ..."
					print
					if not cache.has_key(word):
						search_results = search_query(word, index_file, 0, len(index_file))
						cache[word] = search_results[1]
					else:
						search_results = [word, cache[word]]
					postings.append(search_results[1])
		print "="*100	
		results = combine_postings(postings, True)
		if len(results.keys())>0:
			for key in results:
				if key == 'i':
					print "Infobox results:"
				if key == 'b':
					print "Body results:"
				if key == 't':
					print "Title results:"
				if key == 'c':
					print "Category results:"
				if key == 'r':
					print "References results:"
				if key == 'l':
					print "Links:"
				topList = sorted(results[key].items(), key=operator.itemgetter(1), reverse=True)
				for docID, tf in topList:
					print "\t"+pagemap[docID]
			print 
		else:
			print "Sorry, no results found for "+ origQuery
		print "="*100+"\n"


if __name__=="__main__":
	main()
