from __future__ import print_function
from flask import Flask, render_template
import sys
#from flask.ext.mysqldb import MySQL
#from flaskext.mysql import MySQL
#from flask_mysqldb import MySQl
import pymysql
app = Flask(__name__)
class Database:
        def __init__(self):
                host = "localhost"
                user = "root"
                password = "kanthi"
                db = "insight"
                self.con = pymysql.connect(host=host, user=user, password=password, db=db, cursorclass=pymysql.cursors.DictCursor)
                self.cur = self.con.cursor()
        def list_atrributes(self):
                self.cur.execute("select code_id, repo_name, code_path from hash_signatures limit 10")
                result = self.cur.fetchall()
                if result:
                        print('something worked!', file=sys.stderr)
                        print(result, file=sys.stderr)
                else:
                       print('didnt work', file=sys.stderr)
                return result
 
@app.route('/')
def employees():

        def db_query():
                db = Database()
                emps = db.list_atrributes()
                return emps
        res = db_query()
         return render_template('table.html', result=res, content_type='application/json')

if __name__ =='__main__':
app.run(debug=True)
