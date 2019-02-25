from __future__ import division
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, IntegerType, LongType
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from bisect import bisect_right
from heapq import heappop, heappush
import csv
import re
import os
#import MySQLdb
import sys
import re
import random
import time
import binascii
import MySQLdb
from .flaskapp import Database
import configparser

conf = (SparkConf().setMaster("spark://ec2-34-239-35-112.compute-1.amazonaws.com:7077").setAppName("GitHub-Data").set("spark.executor.memory", "6gb"))
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
gihub_data = "s3n://samplecontentrepos/test*.json"
df = sqlContext.read.json(gdelt_bucket)
df.printSchema()
c_backslash = '\\'
c_dquote = '"'
c_comment = '#'
def chop_comment(allLine):
	allLine1 = allLine.encode('utf8')
	lineList = allLine1.split("\n")
	editedLine = ""
	for item in lineList:
		flag = 0
		in_quote = False  # whether we are in a quoted string right now
		backslash_escape = False  # true if we just saw a backslash
		for i, ch in enumerate(item):
			if not in_quote and ch == c_comment:
            # not in a quote, saw a '#', it's a comment.  Chop it and return!
				editedLine+= item[:i]
				flag = 1
			elif backslash_escape:
            # we must have just seen a backslash; reset that flag and continue
				backslash_escape = False
			elif in_quote and ch == c_backslash:
            # we are in a quote and we see a backslash; escape next char
				backslash_escape = True
			elif ch == c_dquote:
				in_quote = not in_quote
		if flag == 0:
			editedLine=editedLine+item

	return editedLine
#sc.udf.register("choppingthecomments", chop_comment)
func_udf = udf(chop_comment, StringType())
df = df.filter(df.content.isNotNull())
#df = df.filter(df.sample_path.split(".")[1]==py)
cols = F.split(df.sample_path, '\.')
df = df.withColumn('python_files_only', cols.getItem(1))
#df.select('python_files_only').show()
df = df.filter(df.python_files_only == 'py')
df = df.withColumn('comment_chopped_code', func_udf("content"))
#df.select('comment_chopped_code').show()
df.show()

numHashes = 10;
#numDocs = 36
#dataFile = "preprocessed_data.txt"
#plagiaries = {}
#print("Shingling articles...")
curShingleID = 0
docsAsShingleSets = {};
#f = open(dataFile, "r")

print('\nGenerating random hash functions...')
maxShingleID = 2 ** 32 - 1
nextPrime = 4294967311
def pickRandomCoeffs(k):
    randList = []

    while k > 0:
        randIndex = random.randint(0, maxShingleID)

        # Ensure that each random number is unique.
        while randIndex in randList:
            randIndex = random.randint(0, maxShingleID)

            # Add the random number to the list.
        randList.append(randIndex)
        k = k - 1

    return randList


def creartingCoefficientA():
	coeffA = pickRandomCoeffs(numHashes)
	return coeffA

def creatingCoefficientB():
	coeffB = pickRandomCoeffs(numHashes)
	return coeffB




coeffA = creartingCoefficientA()
coeffB = creatingCoefficientB()
db = Database()
cur = db.cur
#cur.execute("show tables")
#for row in cur.fetchall():
#   print row[0]
#allCoeff = coeffA + coeffB
i = 0
for i in range(0, len(coeffA)):
                sql = "insert into randomly_generated_values(hashid, value_a, value_b) values (%s, %s, %s)"
                val = (i+1, coeffA[i], coeffB[i])
               # cur.execute(sql,val)
db.commit()
db.close()


db = Database()
cur = db.cursor()
sql = "select hashid, value_a, value_b from  randomly_generated_values"
cur.execute(sql)
coefficients = cur.fetchall()
valueA = []
valueB = []
print("getting coefficients from database")
for row in coefficients:
	valueA.append(row[1])
	valueB.append(row[2])
db.close()
print("getting data from database")
print(valueA)
print(valueB)

print('\nGenerating MinHash signatures for all documents...')

signatures = []
#docIdList = []

def generate_shingel_minhash(content):
	#docNames = []
	t0 = time.time()
	totalShingles = 0

	words = content.encode('utf-8').split()
	#docID = words[0]
	#docNames.append(docID)
	#del words[0]
	shinglesInDoc = set()
	for index in range(0, len(words) - 2):
        	shingle = words[index] + " " + words[index + 1] + " " + words[index + 2]
        	crc = binascii.crc32(shingle) & 0xffffffff
        	shinglesInDoc.add(crc)
	#docsAsShingleSets[docID] = shinglesInDoc
	print("Shingles in Doc")
	print(shinglesInDoc)
	totalShingles = totalShingles + (len(words) - 2)
	#for docID in docNames:
   # docIdList.append(docID)
    		#temp  = codeIdPath.get(docID)
    		#repoName = temp[0]
    		#repoPath = temp[1]
    # Get the shingle set for this document.
    	#shingleIDSet = docsAsShingleSets[docID]
	uniqueShingles = len(shinglesInDoc)
    # The resulting minhash signature for this document.
    	signature = []
   # signature.append(docID)
   # signature.append(repoName)
   # signature.append(repoPath)
    # For each of the random hash functions...
    	for i in range(0, numHashes):
        	minHashCode = nextPrime + 1
        	for shingleID in shinglesInDoc:
            		hashCode = (valueA[i] * shingleID + valueB[i]) % nextPrime
            	if hashCode < minHashCode:
                	minHashCode = hashCode
        	signature.append(minHashCode)
    #signatures.append(signature)
	elapsed = (time.time() - t0)
	return signature
	#print(signatures)

func_udf = udf(generate_shingel_minhash, ArrayType(LongType()))
df = df.withColumn('ten_signatures', func_udf("content"))
#df.select('ten_signatures').show()



df.printSchema()
def insert_db(df2):
	config = configparser.ConfigParser()
	config.read('config.ini')
	url = "jdbc:mysql://localhost/insight"
	properties = {
        "user": config['mysqlDB']['user'],
        "password": config['mysqlDB']['pass'],
        "driver": config['mysqlDB']['driver']
    }
	df2.write.jdbc(url=url, table="hash_signatures", mode='overwrite', properties=properties)

writeData = df.select("id", "sample_repo_name", "sample_path", df.ten_signatures[0], df.ten_signatures[1], df.ten_signatures[2], df.ten_signatures[3], df.ten_signatures[4], df.ten_signatures[5], df.ten_signatures[6], df.ten_signatures[7], df.ten_signatures[8], df.ten_signatures[9])
insert_db(writeData)
