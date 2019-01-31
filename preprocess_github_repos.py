# filePointer=  open(r"C:\Users\asgam\PycharmProjects\insight_project\Data\results-20190124-124325_s3.csv", encoding="utf8")
# count = 0
# for line in filePointer:
#     count = count +1
#     data = line.split(",")
import csv
# from smart_open import smart_open



c_backslash = '\\'
c_dquote = '"'
c_comment = '#'


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
commentCharacter = "#"
with open(r"C:\Users\asgam\PycharmProjects\insight_project\Data\results-20190124-124325.csv", encoding = "utf8") as csv_file:
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

                print("after removing comments")
                print(updated_code2)
                # if not li.startswith("#"):
                #     python_files[row[0]] = li
                # python_files[row[0]] = row[2]
            count+=1
# print(python_files)
# for y in python_files:
#         print (y,':',python_files[y])

with open('processed_data.txt', 'w',  encoding='utf-8') as f:
    for key in python_files.keys():
        f.write("%s %s\n"%(key,python_files[key]))


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