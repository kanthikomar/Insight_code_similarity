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



#conf = (SparkConf().setMaster("spark://ec2-34-239-35-112.compute-1.amazonaws.com:7077").setAppName("GDELT-News").set("spark.executor.memory", "6gb"))
conf = (SparkConf().setMaster("local").setAppName("GDELT-News").set("spark.executor.memory", "6gb"))

sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
#conf = (SparkConf().setMaster("local").setAppName("SFO_Search").set("spark.exe
#gdelt_bucket = "s3n://samplecontentrepos/results-20190124-124325_s3.csv"
gdelt_bucket = "s3n://samplecontentrepos/test000000000000.json"
#gdelt_bucket = "test.txt"
df = sqlContext.read.json(gdelt_bucket)
#df = sqlContext.read.format('com.databricks.spark.csv').option('maxColumns', 2048000).option('header','true').option( "inferschema",'true').option("delimiter" ,',').load(gdelt_bucket)
df.printSchema()
#df.select("id").show()
#df.select("content").show()
#df.select("sample_path").show()
#df.select("sample_ref").show()
#df.select("sample_repo_name").show()
#spark.read.option("maxColumns", n).csv(...)
#github  = sqlContext.read.csv(gdelt_bucket, header = True).rdd
#pattern = r',(?=")'
#github.map(lambda x: re.split(pattern, x)).collect()
#print github
#guthub.take(20).foreach(println)
#df = sqlContext.read \
#    .format('com.databricks.spark.csv') \
#    .options(header='true') \
#    .options(delimiter=",") \
#    .load(gdelt_bucket)
#df.show()

#for row in df.rdd.collect():
#	print r
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

#def createCodeIdCodePathDictionary():
#   with open(r"results-20190124-124325.csv") as csv_file:
#        codeIdCodePath = {}
#        csv_reader = csv.reader(csv_file,delimiter = "," )
#        count = 0
#        for row in csv_reader:
#            if count == 0:
#                count +=1
#            else:
#                temp = []
#                temp.append(row[5])
#                temp.append(row[7])
#                codeIdCodePath[row[0]] = temp
#    print("dict")
#    print(codeIdCodePath)
#    return codeIdCodePath

numHashes = 10;
#numDocs = 36
#dataFile = "preprocessed_data.txt"
#plagiaries = {}
#print("Shingling articles...")
curShingleID = 0
docsAsShingleSets = {};
#f = open(dataFile, "r")

#f.close()
#print('\nShingling ' + str(numDocs) + ' docs took %.2f sec.' % (time.time() - t0))
#print('\nAverage shingles per doc: %.2f' % (totalShingles / numDocs))
#numElems = int(numDocs * (numDocs - 1) / 2)
#JSim = [0 for x in range(numElems)]
#estJSim = [0 for x in range(numElems)]
#def getTriangleIndex(i, j):
#    if i == j:
#        sys.stderr.write("Can't access triangle matrix with i == j")
#        sys.exit(1)
    # If j < i just swap the values.
#    if j < i:
#        temp = i
#        i = j
#        j = temp
#
#    k = int(i * (numDocs - (i + 1) / 2.0) + j - i) - 1

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
db = MySQLdb.connect(host = "localhost",user = "root",passwd = "kanthi",db = 'insight')
cur = db.cursor()
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

#func_udf1 = udf(creartingCoefficientA, ArrayType(IntegerType()))
#df = df.withColumn('coeffecientA', func_udf1())

#func_udf2 = udf(creatingCoefficientB, ArrayType(IntegerType()))
#df = df.withColumn('coeffecientB', func_udf2())

#df.printSchema()
#df.select('coeffecientB').show()

db = MySQLdb.connect(host = "localhost", user = "root", passwd = "kanthi", db = "insight")
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
#def insert_db(df2):
#    url = "jdbc:mysql://localhost/insight"
#    properties = {
#        "user": "root",
#        "password": "kanthi",
#        "driver": "com.mysql.jdbc.Driver"
#    }
#    df2.write.jdbc(url=url, table="hash_signatures", mode='overwrite', properties = properties)


#writeData = df.select("id", "sample_repo_name", "sample_path", df.ten_signatur$




#allCoeff = coeffA + coeffB
print('\nGenerating MinHash signatures for all documents...')
#codeIdPath = createCodeIdCodePathDictionary()
# List of documents represented as signature vectors
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
#db = MySQLdb.connect(host = "localhost", user = "kanthi_db", 
#cur = db.cursor()
#cur.execute("show tables")
#for row in cur.fetchall():
#   print row[0]

#for item in signatures:
#		sql = "insert into hash_signatures (code_id,repo_name, code_path,signature1,signature2, signature3, signature4, signature5,signature6,signature7,signature8, signature9,signature10) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#		val = (item[0], item[1], item[2], item[3], item[4], item[5],item[6], item[7], item[8], item[9], item[10], item[11], item[12])
#		cur.execute(sql,val)
#for i in range(0, 10):
#		sql = "insert into randomly_generated_values(hashid, value_a, value_b) values (%s,%s,%s)"
#		val = (i+1, allCoeff[i], allCoeff[i+10])
#		cur.execute(sql,val)
#db.commit()

#db.close()
func_udf = udf(generate_shingel_minhash, ArrayType(LongType()))
df = df.withColumn('ten_signatures', func_udf("content"))
#df.select('ten_signatures').show()


#hostname = "localhost"
#dbname = "insight"
#jdbcPort = "3306"
#username = "root"
#password = "kanthi"
#jdbc_url = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(hostname,jdbcPort,dbname,username,password)
#query = "(select * from randomly_generated_values) new_column"
#df1 = sqlContext.read.format('jdbc').options(driver = 'com.mysql.jdbc.Driver',url=jdbc_url, dbtable=query).load()
#df1.show()
#conf = (SparkConf().setMaster("local").setAppName("SFO_Search").set("spark.executor.memory", "6gb"))
#sc = SparkContext(conf = conf)
#sqlContext = SQLContext(sc)
df.printSchema()
def insert_db(df2):
    url = "jdbc:mysql://localhost/insight"
    properties = {
        "user": "root",
        "password": "kanthi",
        "driver": "com.mysql.jdbc.Driver"
    }
    df2.write.jdbc(url=url, table="hash_signatures", mode='overwrite', properties=properties)

writeData = df.select("id", "sample_repo_name", "sample_path", df.ten_signatures[0], df.ten_signatures[1], df.ten_signatures[2], df.ten_signatures[3], df.ten_signatures[4], df.ten_signatures[5], df.ten_signatures[6], df.ten_signatures[7], df.ten_signatures[8], df.ten_signatures[9])
#writeData.write.jdbc(table="hash_signatures", properties= properties, mode = "append")

#df = DataFrame([{'apple','122','121'}, {'banana', '112', '121'}], columns=["h$
#df2 = [{'hashid':'pineapple', 'value_a': 1212, 'value_b': 4654}]
#sdf = sqlContext.createDataFrame(df2)
insert_db(writeData)
