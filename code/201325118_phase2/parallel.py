import xml.sax
from nltk.stem import PorterStemmer
import re
from nltk.corpus import stopwords
from collections import Counter
import multiprocessing as mp
import sys
import operator 
import hashlib
import pickle
import os

sw = stopwords.words('english')
porter=PorterStemmer()

procs = mp.cpu_count()		# Total number of processors on this system
process_queue = mp.Queue()	# Queue which holds every page extracted from XML for further processing
workers = []				# List that keeps track of worker processes
docs = []					# List that holds all documents extracted from XML with their attributes
pagemap = {}				# Document ID - Document title mapping 

'''
Distributed function to process a page extracted from XML file from the process_queue and generate word
counts after running through extraction-tokenization-stemming-stopwords_removal pipeline.
'''
def process_page(process_queue, data_queue):
	while (1):
		currentDoc = process_queue.get()
		index = {}

		if currentDoc == 'DONE':
			process_queue.put("DONE")
			data_queue.put("DONE")
			return True

		if len(currentDoc.text.split('== References ==')) > 1:
			currentDoc.extra = '== References ==' + currentDoc.text.split('== References ==')[1]
			currentDoc.text = currentDoc.text.split('== References ==')[0]
			currentDoc.stemmed = tokenize(currentDoc.text)
			currentDoc.references = tokenize(get_refs(currentDoc.extra))
			currentDoc.links = tokenize(get_links(currentDoc.extra))
			currentDoc.categories = tokenize("\n".join(get_categories(currentDoc.extra)))
		else:
			currentDoc.stemmed = tokenize(currentDoc.text)
			currentDoc.references = tokenize(get_refs(currentDoc.text))
			currentDoc.links = tokenize(get_links(currentDoc.text))
			currentDoc.categories = tokenize("\n".join(get_categories(currentDoc.text)))
		currentDoc.infobox = tokenize(get_infobox(currentDoc.text))
		cnt_stemmed = Counter()
		cnt_cats = Counter()
		cnt_links = Counter()
		cnt_ibox = Counter()
		cnt_refs = Counter()

		for word in currentDoc.stemmed:					
			cnt_stemmed[word] += 1								
		for word in currentDoc.categories:					
			cnt_cats[word] += 1
		for word in currentDoc.links:					
			cnt_links[word] += 1
		for word in currentDoc.references:					
			cnt_refs[word] += 1
		for word in currentDoc.infobox:					
			cnt_ibox[word] += 1
		
		to_save = {"id": currentDoc.id, "title": currentDoc.title, "cnt_stemmed": cnt_stemmed, "cnt_cats": cnt_cats, "cnt_links": cnt_links, "cnt_ibox": cnt_ibox, "cnt_refs": cnt_refs}
		data_queue.put(to_save)


'''
Function to store the index in an output file 
in a compressed format so as to reduce filesize.
'''
def save_data(ofp, index, close=1):
	sortedIndex = sorted(index)
	vals = [index[i] for i in sortedIndex]
	for i in xrange(0, len(sortedIndex)):
		word = sortedIndex[i]
		wordVal = vals[i]
		if wordVal.has_key('b'):
			wordVal["b"].sort(key=operator.itemgetter(1), reverse=True)
		if index[word].has_key('l'):
			wordVal["l"].sort(key=operator.itemgetter(1), reverse=True)
		if index[word].has_key('r'):
			wordVal["r"].sort(key=operator.itemgetter(1), reverse=True)
		if index[word].has_key('i'):
			wordVal["i"].sort(key=operator.itemgetter(1), reverse=True)
		if index[word].has_key('c'):
			wordVal["c"].sort(key=operator.itemgetter(1), reverse=True)
		ofp.write(word.encode('ascii', 'ignore')+":")
		for each in wordVal:
			ofp.write("_"+each)
			for doc in wordVal[each]:
				ofp.write("|"+str(doc[0])+","+str(doc[1]))
		ofp.write("\n")
	if close == 1:
		ofp.close()


'''
Retrieves the postings for a particular line in the index
into standard data structure for this program.
'''
def get_data(line):
	result = {}
	# print "LINE-", line
	if isinstance(line, list):
		return line
	line = line.split(":")
	vals = line[-1]
	word = ''.join(line[:-1])
	for each in vals.split("_")[1:]:
	 	result[each[0]] = []	
		for pos in each[2:].split("|"):
			result[each[0]].append([int(x) for x in pos.split(',')])
	return [word, result]


'''
Merges two postings according to the tf for the documents
'''
def merge_postings(p1, p2):
	result = []
	i = 0
	j = 0
	while i < len(p1) and j < len(p2):
		doc1 = p1[i][0]
		freq1 = p1[i][1]
		doc2 = p2[j][0]
		freq2 = p2[j][1]
		while doc1 != doc2:
			if freq1 > freq2:
				result.append([doc1, freq1])
				i+=1
			else:
				result.append([doc2, freq2])
				j+=1
			if i < len(p1) and j < len(p2):
				doc1 = p1[i][0]
				freq1 = p1[i][1]
				doc2 = p2[j][0]
				freq2 = p2[j][1]
			else: 
				break
		if doc1 == doc2:
			result.append([doc1, freq1+freq2])
			i+=1 
			j+=1
	while i < len(p1):
		doc1 = p1[i][0]
		freq1 = p1[i][1]
		result.append([doc1, freq1])
		i+=1
	while j < len(p2):
		doc2 = p2[j][0]
		freq2 = p2[j][1]
		result.append([doc2, freq2])
		j+=1

	return result


'''
Distributed process for merging index files
using Single pass in memory indexing (SPIMI)
'''
def merge_index(destFile, merge_queue):
	while(1):
		result = {}
		i = merge_queue.get()
		try:
			j = merge_queue.get(timeout=5)
		except:
			print "Exiting"
			merge_queue.put(i)
			return True
		newIndex = hashlib.sha224(str(i)).hexdigest()
		print "Merging files "+str(i)+ " and " + str(j)
		f1 = open(destFile+"_"+str(i), "r")
		try:
			f2 = open(destFile+"_"+str(j), "r")
		except IOError:
			return True
		fout = open(destFile+"_"+str(newIndex), "w")
		buff1 = f1.readline()
		buff2 = f2.readline()
		while buff1 and buff2:
			result = {}
			buff1 = get_data(buff1)
			buff2 = get_data(buff2)
			while buff1[0] != buff2[0]:
				if (buff1[0] < buff2[0]):
					save_data(fout, {buff1[0]:buff1[1]}, 0)
					buff1 = f1.readline()
					if buff1:
						buff1 = get_data(buff1)
					else:
						break
				else:
					save_data(fout, {buff2[0]:buff2[1]}, 0)
					buff2 = f2.readline()
					if buff2:
						buff2 = get_data(buff2)
					else: 
						break

			if buff1 and buff2 and buff1[0] == buff2[0]:
				result = { buff1[0]: {} }
				for cat in buff1[1]:
					vals1 = buff1[1][cat]
					if buff2[1].has_key(cat):
						vals2 = buff2[1][cat]
						del buff2[1][cat]

					else:
						vals2 = []
					result[buff1[0]][cat] = merge_postings(vals1, vals2)

				for cat in buff2[1]:
					vals1 = buff2[1][cat]
					result[buff1[0]][cat] = merge_postings(vals1, [])
				save_data(fout, result, 0)

			buff1 = f1.readline()
			buff2 = f2.readline()
		
		if not buff1 and not buff2:
			fout.close()

		while buff1:
			buff1 = get_data(buff1)
			result = { buff1[0]: {} }
			for cat in buff1[1]:
				vals1 = buff1[1][cat]
				result[buff1[0]][cat] = merge_postings(vals1, [])
			save_data(fout, result, 0)	
			buff1 = f1.readline()
			if not buff1 and not buff2:
				fout.close()

		while buff2:
			buff2 = get_data(buff2)
			result = { buff2[0]: {} }
			for cat in buff2[1]:
				vals2 = buff2[1][cat]
				result[buff2[0]][cat] = merge_postings(vals2, [])
			save_data(fout, result, 0)	
			buff2 = f2.readline()
			if not buff2:
				fout.close()

		while not fout.closed:
			pass
		merge_queue.put(newIndex)
		os.remove(destFile+"_"+str(j))


'''
Distributed over 1 process function that will take the generated 
word counts and put them in an inverted index format.
'''
def combiner(data_queue, index, destFile, fileCounter): 
	count = 0

	while (1):
		data = data_queue.get()
		if data == 'DONE':
			count += 1
			if count == procs-1:
				dest = open(destFile+'_'+str(fileCounter), "w")
				save_data(dest, index)
				fileCounter += 1
				while not dest.closed:
					pass
				data_queue.put(fileCounter)
				return True
		else:
			docID = data["id"]
			title = data["title"]
			cnt_links = data["cnt_links"]
			cnt_ibox = data["cnt_ibox"]
			cnt_refs = data["cnt_refs"]
			cnt_cats = data["cnt_cats"]
			cnt_stemmed = data["cnt_stemmed"]

			if index.has_key(title):
				if index[title].has_key("t"):
					index[title]["t"].append((docID, 1))
				else:
					index[title]["t"] = [(docID, 1)]
			else:
				index[title] = {"t": [(docID, 1)]}

			for word in cnt_cats:
				if index.has_key(word):
					if index[word].has_key("c"):
						index[word]["c"].append((docID, cnt_cats[word]))
					else:
						index[word]["c"] = [(docID, cnt_cats[word])]
				else:
					index[word] = {"c": [(docID, cnt_cats[word])]}

			for word in cnt_ibox:
				if index.has_key(word):
					if index[word].has_key("i"):
						index[word]["i"].append((docID, cnt_ibox[word]))
					else:
						index[word]["i"] = [(docID, cnt_ibox[word])]
				else:
					index[word] = {"i": [(docID, cnt_ibox[word])]}

			for word in cnt_refs:
				if index.has_key(word):
					if index[word].has_key("r"):
						index[word]["r"].append((docID, cnt_refs[word]))
					else:
						index[word]["r"] = [(docID, cnt_refs[word])]
				else:
					index[word] = {"r": [(docID, cnt_refs[word])]}

			for word in cnt_links:
				if index.has_key(word):
					if index[word].has_key("l"):
						index[word]["l"].append((docID, cnt_links[word]))
					else:
						index[word]["l"] = [(docID, cnt_links[word])]
				else:
					index[word] = {"l": [(docID, cnt_links[word])]}

			for word in cnt_stemmed:
				if index.has_key(word):
					if index[word].has_key("b"):
						index[word]["b"].append((docID, cnt_stemmed[word]))
					else:
						index[word]["b"] = [(docID, cnt_stemmed[word])]
				else:
					index[word] = {"b": [(docID, cnt_stemmed[word])]}
			
		# print str(len(index.keys()))

		if len(index.keys()) >= 100000:
			print str(fileCounter+1), "index files saved."
			dest = open(destFile+'_'+str(fileCounter), "w")
			save_data(dest, index)
			fileCounter += 1
			index = {}


'''
Function to tokenize, stem and remove stopwords from raw text
'''
def tokenize(text):
	text=text.lower()
	text=re.sub(r'[^a-z0-9 ]',' ',text) 
	text=text.split()
	text=[x for x in text if x not in sw]  
	text=[ porter.stem(word) for word in text]
	return text

'''
Function to extract references from raw text
'''
def get_refs(text):
	results = re.search('== References ==(.*?)==', text, re.DOTALL)
	if results is not None:
		return results.group(1).strip()
	else:
		return ""

'''
Function to extract links from raw text
'''
def get_links(text):
	results = re.search('==External links==(.*?)\n\n', text, re.DOTALL)
	if results is not None:
		return results.group(1).strip()
	else:
		return ""

'''
Function to extract categories from raw text
'''
def get_categories(text):
	results = re.findall('\[\[Category:(.*?)\]\]', text)
	if results is not None:
		return results
	else:
		return ""

'''
Function to extract infobox from raw text
'''
def get_infobox(text):
	results = re.search('{{Infobox(.*?)}}', text, re.DOTALL)
	if results is not None:
		return results.group(1).strip()
	else:
		return ""


class Document(object):										# Custom class definition for a document parsed from XML
	def __init__(self):
		self.id = 0
		self.title = ""
		self.text = ""
		self.stemmed = []
		self.extra = ""
		self.links = []
		self.references = []
		self.categories = []

	def setTitle(self, value):
		self.title = value

class wikiContentHandler(xml.sax.ContentHandler):			# Handler for the XML parser
	def __init__(self):
		xml.sax.ContentHandler.__init__(self)
		self.currentTag = ""

	def startElement(self, name, attrs):					# An element start tag encountered.
		self.currentTag = name
		if name == "page":
			newDoc = Document()
			self.currentDoc = newDoc

	def endElement(self, name):								# An element end tag encountered.
		if name == "page":
			process_queue.put(self.currentDoc)
			# docs.append(self.currentDoc)		# Might need to remove this to get scalability. 
			pagemap[int(self.currentDoc.id)] = self.currentDoc.title

	def characters(self, content):							# Text between the start tag and end tag.
		if self.currentTag == "title":
			self.currentDoc.title += content.strip().lower()
		elif self.currentTag == "id" and self.currentDoc.id==0:
			self.currentDoc.id = content.strip()
		elif self.currentTag == "text":
			self.currentDoc.text += content


def main(sourceFile, destFile):
	global docs
	global workers

	index = {}
	fileCounter = 0
	data_queue = mp.Queue()
	merge_queue = mp.Queue()
	c = mp.Process(target=combiner, args=(data_queue,index, destFile,fileCounter,))				# Combiner process
	c.start()

	for w in xrange(procs-1):
		p = mp.Process(target=process_page, args=(process_queue,data_queue,))		# Distributed page processing 
		p.start()
		workers.append(p)

	source = open(sourceFile)
	xml.sax.parse(source, wikiContentHandler())
	process_queue.put("DONE")
	for w in workers:
		w.join()
	c.join()
	fileCounter = int(data_queue.get())
	print "Starting merge process for " + str(fileCounter) + " files..."
	for x in xrange(0, fileCounter):
		merge_queue.put(x)

	workers = []
	for w in xrange(procs-1):
		p = mp.Process(target=merge_index, args=(destFile, merge_queue,))		# Distributed index merging
		p.start()
		workers.append(p)

	for w in workers:
		w.join()

	workers = []
	for w in xrange(1):
		p = mp.Process(target=merge_index, args=(destFile, merge_queue,))		# Distributed index merging over one process, handles residual merges. 
		p.start()
		workers.append(p)

	for w in workers:
		w.join()
	os.rename(destFile+ "_" +str(merge_queue.get()), destFile)
	print "Merging complete. Final file: " + destFile
	pickle.dump( pagemap, open( destFile+"_pagemap", "wb" ) )

if __name__ == "__main__":
	main(sys.argv[1], sys.argv[2])
