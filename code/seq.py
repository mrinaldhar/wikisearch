import xml.sax
from nltk.stem import PorterStemmer
import re
from nltk.corpus import stopwords
from collections import Counter
import sys

sw = stopwords.words('english')
porter=PorterStemmer()


docs = []

index = {}

def tokenize(text):
	text=text.lower()
	text=re.sub(r'[^a-z0-9 ]',' ',text) 
	text=text.split()
	text=[x for x in text if x not in sw]  
	text=[ porter.stem(word) for word in text]
	return text

def get_refs(text):
	results = re.search('== References ==(.*?)==', text, re.DOTALL)
	if results is not None:
		return results.group(1).strip()
	else:
		return ""

def get_links(text):
	results = re.search('==External links==(.*?)\n\n', text, re.DOTALL)
	if results is not None:
		return results.group(1).strip()
	else:
		return ""

def get_categories(text):
	results = re.findall('\[\[Category:(.*?)\]\]', text)
	if results is not None:
		return results
	else:
		return ""

def get_infobox(text):
	results = re.search('{{Infobox(.*?)}}', text, re.DOTALL)
	if results is not None:
		return results.group(1).strip()
	else:
		return ""

class Document(object):
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

class wikiContentHandler(xml.sax.ContentHandler):
	def __init__(self):
		xml.sax.ContentHandler.__init__(self)
		self.currentTag = ""

	def startElement(self, name, attrs):
		self.currentTag = name
		if name == "page":
			newDoc = Document()
			self.currentDoc = newDoc

	def endElement(self, name):
		if name == "page":
			if len(self.currentDoc.text.split('== References ==')) > 1:
				self.currentDoc.extra = '== References ==' + self.currentDoc.text.split('== References ==')[1]
				self.currentDoc.text = self.currentDoc.text.split('== References ==')[0]
				self.currentDoc.stemmed = tokenize(self.currentDoc.text)
				self.currentDoc.references = tokenize(get_refs(self.currentDoc.extra))
				self.currentDoc.links = tokenize(get_links(self.currentDoc.extra))
				self.currentDoc.categories = tokenize("\n".join(get_categories(self.currentDoc.extra)))
			else:
				self.currentDoc.stemmed = tokenize(self.currentDoc.text)
				self.currentDoc.references = tokenize(get_refs(self.currentDoc.text))
				self.currentDoc.links = tokenize(get_links(self.currentDoc.text))
				self.currentDoc.categories = tokenize("\n".join(get_categories(self.currentDoc.text)))
			self.currentDoc.infobox = tokenize(get_infobox(self.currentDoc.text))
			cnt_stemmed = Counter()
			cnt_cats = Counter()
			cnt_links = Counter()
			cnt_ibox = Counter()
			cnt_refs = Counter()

			for word in self.currentDoc.stemmed:					
				cnt_stemmed[word] += 1								
			for word in self.currentDoc.categories:					
				cnt_cats[word] += 1
			for word in self.currentDoc.links:					
				cnt_links[word] += 1
			for word in self.currentDoc.references:					
				cnt_refs[word] += 1
			for word in self.currentDoc.infobox:					
				cnt_ibox[word] += 1


			if index.has_key(self.currentDoc.title):
				if index[self.currentDoc.title].has_key("t"):
					index[self.currentDoc.title]["t"].append((self.currentDoc.id, 1))
				else:
					index[self.currentDoc.title]["t"] = [(self.currentDoc.id, 1)]
			else:
				index[self.currentDoc.title] = {"t": [(self.currentDoc.id, 1)]}

			for word in cnt_cats:
				if index.has_key(word):
					if index[word].has_key("c"):
						index[word]["c"].append((self.currentDoc.id, cnt_cats[word]))
					else:
						index[word]["c"] = [(self.currentDoc.id, cnt_cats[word])]
				else:
					index[word] = {"c": [(self.currentDoc.id, cnt_cats[word])]}

			for word in cnt_ibox:
				if index.has_key(word):
					if index[word].has_key("i"):
						index[word]["i"].append((self.currentDoc.id, cnt_ibox[word]))
					else:
						index[word]["i"] = [(self.currentDoc.id, cnt_ibox[word])]
				else:
					index[word] = {"i": [(self.currentDoc.id, cnt_ibox[word])]}

			for word in cnt_refs:
				if index.has_key(word):
					if index[word].has_key("r"):
						index[word]["r"].append((self.currentDoc.id, cnt_refs[word]))
					else:
						index[word]["r"] = [(self.currentDoc.id, cnt_refs[word])]
				else:
					index[word] = {"r": [(self.currentDoc.id, cnt_refs[word])]}

			for word in cnt_links:
				if index.has_key(word):
					if index[word].has_key("l"):
						index[word]["l"].append((self.currentDoc.id, cnt_links[word]))
					else:
						index[word]["l"] = [(self.currentDoc.id, cnt_links[word])]
				else:
					index[word] = {"l": [(self.currentDoc.id, cnt_links[word])]}

			for word in cnt_stemmed:
				if index.has_key(word):
					if index[word].has_key("b"):
						index[word]["b"].append((self.currentDoc.id, cnt_stemmed[word]))
					else:
						index[word]["b"] = [(self.currentDoc.id, cnt_stemmed[word])]
				else:
					index[word] = {"b": [(self.currentDoc.id, cnt_stemmed[word])]}

			docs.append(self.currentDoc)

	def characters(self, content):
		if self.currentTag == "title":
			self.currentDoc.title += content.strip()
		elif self.currentTag == "id" and self.currentDoc.id==0:
			self.currentDoc.id = content.strip()
		elif self.currentTag == "text":
			self.currentDoc.text += content

def save_data(ofp):
	for word in index:
		ofp.write(word.encode('ascii', 'ignore')+":")
		for each in index[word]:
			ofp.write("_"+each)
			for doc in index[word][each]:
				ofp.write("|"+str(doc[0])+","+str(doc[1]))
		ofp.write("\n")
	ofp.close()
		


def main(sourceFile, destFile):
	global docs
	source = open(sourceFile)
	xml.sax.parse(source, wikiContentHandler())
	dest = open(destFile, "w")
	save_data(dest)

if __name__ == "__main__":
	main(sys.argv[1], sys.argv[2])
