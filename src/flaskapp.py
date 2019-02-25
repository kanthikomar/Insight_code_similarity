from __future__ import print_function
from flask import Flask, render_template, request
import sys
import pymysql
import MySQLdb
import binascii
import configparser

app = Flask(__name__)

@app.route("/", methods = ['POST','GET'])
def my_form():
        return render_template('first_page.html')

class Database:
        def __init__(self):
                self.config = configparser.ConfigParser()
                self.config.read('config.ini')
                self.con = pymysql.connect(host=self.config['mysqlDB']['host'], user=self.config['mysqlDB']['user'],
                           password=self.config['mysqlDB']['pass'],db=self.config['mysqlDB']['db'],
                                           cursorclass=pymysql.cursors.DictCursor)
                self.cur = self.con.cursor()
        def list_atrributes(self):
                self.cur.execute("select id, sample_repo_name, sample_path from hash_signatures limit 10")
                result = self.cur.fetchall()
                if result:
                        print('something worked!', file=sys.stderr)
                        print(result, file=sys.stderr)
                else:
                       print('didnt work', file=sys.stderr)
                return result

c_backslash = '\\'
c_dquote = '"'
c_comment = '#'

def chop_comment(allLine):
       # allLine1 = allLine.encode('utf8')
        lineList = allLine.split("\n")
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
				backslash_escape = False
                        elif in_quote and ch == c_backslash:
            # we are in a quote and we see a backslash; escape next char
                                backslash_escape = True
                        elif ch == c_dquote:
                                in_quote = not in_quote
                if flag == 0:
                        editedLine=editedLine+item

        return editedLine

numHashes = 10
db = Database()
cur = db.cur
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


maxShingleID = 2 ** 32 - 1
nextPrime = 4294967311
signatures = []
def generate_shingel_minhash(content):
        totalShingles = 0
        words = content.split()
        shinglesInDoc = set()
        for index in range(0, len(words) - 2):
                shingle = words[index] + " " + words[index + 1] + " " + words[index + 2]
                crc = binascii.crc32(shingle) & 0xffffffff
                shinglesInDoc.add(crc)
        totalShingles = totalShingles + (len(words) - 2)
        uniqueShingles = len(shinglesInDoc)
    # The resulting minhash signature for this document.
        signature = []
        for i in range(0, numHashes):
                minHashCode = nextPrime + 1
                for shingleID in shinglesInDoc:
                        hashCode = (valueA[i] * shingleID + valueB[i]) % nextPrime
                    if hashCode < minHashCode:
                        minHashCode = hashCode
                signature.append(minHashCode)
    #signatures.append(signature)
       # elapsed = (time.time() - t0)
        return signature

@app.route('/result', methods = ['POST','GET'] )
def employees():

    def db_query():
            db = Database()
            emps = db.list_atrributes()
            return emps
    if request.method == 'POST':
        res = db_query()
        result = request.form['code']
        commentChoppedCode = chop_comment(result)
        signatureList =  generate_shingel_minhash(commentChoppedCode)
        db = Database()
        cur = db.cur
        sql = "select * from hash_signatures"
        cur.execute(sql)
        numRows = cur.rowcount
        for x in xrange(0, numRows):
            eachCodeSignatures = []
            row = cur.fetchone()
            eachCodeSignatures.append(row[3])
            eachCodeSignatures.append(row[4])
            eachCodeSignatures.append(row[5])
            eachCodeSignatures.append(row[6])
            eachCodeSignatures.append(row[7])
            eachCodeSignatures.append(row[8])
            eachCodeSignatures.append(row[9])
            eachCodeSignatures.append(row[10])
            eachCodeSignatures.append(row[11])
            eachCodeSignatures.append(row[12])
            estimatedSimilarity = len(set(eachCodeSignatures)) and  len(set(signatureList))
            similarityPercentage = estimatedSimilarity/10
            if (similarityPercentage >= 0.5):
                return '''<h1> the result is : {}{}{}</h1>'''.format(row[0], row[1], row[2])
            else:
                return '''<h1> none </h1>'''

		# return.format(commentChoppedCode)
       		# return render_template('check.html', result=commentChoppedCode, content_type='application/json')
		# return render_template('table.html', result=res, content_type='application/json')

if __name__ =='__main__':
	app.run(debug = True)

