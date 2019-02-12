# filePointer=  open(r"C:\Users\asgam\PycharmProjects\insight_project\Data\results-20190124-124325_s3.csv", encoding="utf8")
# count = 0
# for line in filePointer:
#     count = count +1
#     data = line.split(",")
import csv
import random
import binascii
# from smart_open import smart_open



c_backslash = '\\'
c_dquote = '"'
c_comment = '#'

maxShingleID = 2**32-1
numHashes = 10
nextPrime = 4294967311

def pickRandomCoeffs(k):
    # Create a list of 'k' random values.
    randList = []

    while k > 0:
        # Get a random shingle ID.
        randIndex = random.randint(0, maxShingleID)

        # Ensure that each random number is unique.
        while randIndex in randList:
            randIndex = random.randint(0, maxShingleID)

            # Add the random number to the list.
        randList.append(randIndex)
        k = k - 1

    return randList

# For each of the 'numHashes' hash functions, generate a different coefficient 'a' and 'b'.
coeffA = pickRandomCoeffs(numHashes)
coeffB = pickRandomCoeffs(numHashes)

def genMinHashSign(docsAsShingleSets,docNames,):
    # For each document...
    signatures = []
    for docID in docNames:

        # Get the shingle set for this document.
        shingleIDSet = docsAsShingleSets[docID]

        # The resulting minhash signature for this document.
        signature = []

        # For each of the random hash functions...
        for i in range(0, numHashes):

            # For each of the shingles actually in the document, calculate its hash code
            # using hash function 'i'.

            # Track the lowest hash ID seen. Initialize 'minHashCode' to be greater than
            # the maximum possible value output by the hash.
            minHashCode = nextPrime + 1

            # For each shingle in the document...
            for shingleID in shingleIDSet:
                # Evaluate the hash function.
                hashCode = (coeffA[i] * shingleID + coeffB[i]) % nextPrime

                # Track the lowest hash code seen.
                if hashCode < minHashCode:
                    minHashCode = hashCode

            # Add the smallest hash code value as component number 'i' of the signature.
            signature.append(minHashCode)

        # Store the MinHash signature for this document.
        signatures.append(signature)

def generateShingles(Docs):
    totalShingles = 0
    numDocs = len(Docs)
    docsAsShingleSets = {}
    docNames = []
    for i in range(0, numDocs):

        # Read all of the words (they are all on one line) and split them by white
        # space.
        words = f.readline().split(" ")

        # Retrieve the article ID, which is the first word on the line.
        docID = words[0]

        # Maintain a list of all document IDs.
        docNames.append(docID)

        del words[0]

        # 'shinglesInDoc' will hold all of the unique shingle IDs present in the
        # current document. If a shingle ID occurs multiple times in the document,
        # it will only appear once in the set (this is a property of Python sets).
        shinglesInDoc = set()

        # For each word in the document...
        for index in range(0, len(words) - 2):
            # Construct the shingle text by combining three words together.
            shingle = words[index] + " " + words[index + 1] + " " + words[index + 2]

            # Hash the shingle to a 32-bit integer.
            crc = binascii.crc32(shingle) & 0xffffffff

            # Add the hash value to the list of shingles for the current document.
            # Note that set objects will only add the value to the set if the set
            # doesn't already contain it.
            shinglesInDoc.add(crc)

        # Store the completed list of shingles for this document in the dictionary.
        docsAsShingleSets[docID] = shinglesInDoc

        # Count the number of shingles across all documents.
        totalShingles = totalShingles + (len(words) - 2)


def chop_comment(line):
    # a little state machine with two state varaibles:
    in_quote = False  # whether we are in a quoted string right now
    backslash_escape = False  # true if we just saw a backslash

    for i, ch in enumerate(line):
        if not in_quote and ch == c_comment:
            # not in a quote, saw a '#', it's a comment.  Chop it and return!
            return line[:i]
        elif backslash_escape:
            # we must have just seen a backslash; reset that flag and continue
            backslash_escape = False
        elif in_quote and ch == c_backslash:
            # we are in a quote and we see a backslash; escape next char
            backslash_escape = True
        elif ch == c_dquote:
            in_quote = not in_quote

    return line
codeDictionary = {}
commentCharacter = "#"
with open(r"results-20190124-124325.csv", encoding = "utf8") as csv_file:
    python_files = {}
    csv_reader = csv.reader(csv_file, delimiter=",")
    count = 0
    for row in csv_reader:
        if count == 0:
            # print(f'Column names are {", ".join(row)}')
            count += 1
        else:
            # print(f'\t{row[7]}')
            temp = row[7].split(".")
            if (len(temp) > 1 and temp[1] =='py'):
                # print(count)
                # print(f'\t{row[7]}')
                id = row[0]
                code = row[2].split('\n')
                updated_code = list(filter(None, code))
                for i in range(0, len(updated_code)):
                    item = updated_code[i]
                    item2 = chop_comment(item)
                    updated_code[i] = item2
                updated_code2 = list(filter(None, updated_code))
                    # firstCharacter = item[0]
                    # if firstCharacter == '#' or firstCharacter == '"':
                    #     updated_code.remove(item)

                    #     i+=1
                    #     updatedCodeLen = len(updated_code)
                    # i+=1

                # print("after removing comments")
                # print(' '.join(updated_code2))
                codeDictionary[id] = ' '.join(updated_code2)

                # if not li.startswith("#"):
                #     python_files[row[0]] = li
                # python_files[row[0]] = row[2]
            count+=1
# print(python_files)
# for y in python_files:
#         print (y,':',python_files[y])
print(codeDictionary)
print(len(codeDictionary))
with open('processed_data.txt', 'w',  encoding='utf-8') as f:
    for key in codeDictionary.keys():
        f.write("%s %s\n"%(key,codeDictionary[key]))


# with smart_open('s3://samplecontentrepos/test_000000000000.csv', 'rb') as fin:
#     for line in fin:
#         print(line)

# from pyspark import SparkContext
# from pyspark.sql import SQLContext
# from pyspark import SparkConf
# from pyspark.sql import SparkSession
# import pandas
# SparkConf().set("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.0.0-alpha3")
# sc = SparkContext.getOrCreate()
# sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "WWWWA")
# sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "QQQQ")
# sqlContext = SQLContext(sc)
# df2 = sqlContext.read.csv("s3a://insightstackoverflowsample/Test/Test_0")
#df.show()