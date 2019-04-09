import zipfile, urllib.request, shutil
import datetime, pdb
from datetime import timedelta
import sys, os
import fileinput
import csv, redis, json
import sys, cherrypy
import random
from redis import StrictRedis
import multiprocessing as mp
import itertools
import time


# print ()
# print ("Current date and time using str method of datetime object:")
# print (str(now))

# print ()
# print ("Current date and time using instance attributes:")
# print ("Current year: %d" % now.year)
# print ("Current month: %d" % now.month)
# 
# 
# 
# print ("sadd")
# print (a, b, c[2:4])
# print ("Current day: %d" % now.day)
# print ("Current hour: %d" % now.hour)
# print ("Current minute: %d" % now.minute)
# print ("Current second: %d" % now.second)
# print ("Current microsecond: %d" % now.microsecond)

# print ()
# print ("Current date and time using strftime:")
# print (now.strftime("%Y-%m-%d %H:%M"))

# print ()
# print ("Current date and time using isoformat:")
# print (now.isoformat())
REDIS_HOST = 'localhost'

def read_csv_data(csv_file, ik, iv):
	with open(csv_file, encoding='utf-8') as csvf:
		csv_data = csv.reader(csvf)
		return [(r[ik], r[iv]) for r in csv_data]

def store_data(conn, data):
	for i in data:
		conn.setnx(i[0], i[1])
	return data

def find_bad_qn(a, url, file_name):
	try:
		with urllib.request.urlopen(url) as response, open(file_name, 'wb') as out_file:
			shutil.copyfileobj(response, out_file)
			with zipfile.ZipFile(file_name) as zf:
				zf.extractall('Files')
		return 0
	except:
		return 1

def main():
	now = datetime.datetime.now() - timedelta(1)
	day = '%02d' % now.day
	month = '%02d' % now.month
	year = '%02d' % now.year
	fileName = 'Files/EQ' + day + month + year[2:4] + '.csv'
	# pdb.set_trace()
	url = 'https://www.bseindia.com/download/BhavCopy/Equity/EQ'+ day + month + year[2:4] +'_CSV.ZIP'
	file_name = 'Files/myzip.zip'
	countx = 0
	for i in range(298314,298346):
		check = find_bad_qn(i, url, file_name)
		if check == 1:
			countx += 1
			if countx % 10 == 0:
				now = now - timedelta(1)
				day = '%02d' % now.day
				month = '%02d' % now.month
				year = '%02d' % now.year
				fileName = 'Files/EQ' + day + month + year[2:4] + '.csv'
				# pdb.set_trace()
				url = 'https://www.bseindia.com/download/BhavCopy/Equity/EQ'+ day + month + year[2:4] +'_CSV.ZIP'
		else:
			break
	# if len(sys.argv) < 2:
	# 	sys.exit(
	# 		"Usage: %s  [key_column_index, value_column_index]"
	# 		% __file__
	# 	)
	# columns = (0, 1) if len(sys.argv) < 4 else (int(x) for x in sys.argv[2:4])
	# data = read_csv_data('EQ190319.CSV', 1, 15)
	conn = redis.Redis(REDIS_HOST)
	count = 1
	with open(fileName) as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=',')
		line_count = 0
		for row in csv_reader:
			# pdb.set_trace()
			if count == 1:
				count += 1
				header = row
			else:
				hashKey = row[1].upper()
				# print(hashKey)
				hashKey = hashKey.replace(" ", "")
				# print(hashKey)
				index = 0
				for head in header:
					conn.hset(hashKey, head, row[index])
					index += 1
				# print(conn.hgetall(hashKey))
	os.remove(fileName)
	os.remove(file_name)
		
class StringGenerator(object):
	@cherrypy.expose
	def index(self):
		main()
		return open('index.html')


@cherrypy.expose
class StringGeneratorWebService(object):

	@cherrypy.tools.accept(media='text/plain')
	def GET(self):
		conn = redis.StrictRedis(host=REDIS_HOST, decode_responses=True)
		# namevaluex = conn.hgetall('500002')
		# namevaluextest = conn.hgetall()
		# print(namevaluex['SC_CODE'])
		 # Let's make up a list of users for presentation purposes
		# users = ['Remi', 'Carlos', 'Hendrik', 'Lorenzo Lamas']

		# Every yield line adds one part to the total result body.
		# yield self.header()
		# yield '<h3>List of users:</h3>'

		
		#print('conn ',conn.keys(), type(conn.keys()))
		# pdb.set_trace()
		dataShow = []
		for i,user in enumerate(conn.keys()):
			temp = conn.hgetall(user)
			# temp = json.dumps(temp)
			dataShow.append(temp)
			# print(dataShow)
			if i==9:
				break
		# print(dataShow)
		# pdb.set_trace()
		# for i in conn.keys():
		# 	print('i',i)
		# 	print('%s<br/>', conn.hgetall(i))
		# 	print(type(conn.hgetall(i)))
		# 	yield '%s<br/>' % conn.hgetall(i)
		# 	break

		return json.dumps(dataShow)
	def POST(self, another_string):
		# pdb.set_trace()
		dataShow = []
		conn = redis.StrictRedis(host=REDIS_HOST, decode_responses=True)
		val = "*" + another_string.upper() + "*"
		count = 0
		for user in conn.keys(val):
			temp = conn.hgetall(user)
			dataShow.append(temp)
			count+=1
			if count == 9:
				break
		# print(dataShow)
		return json.dumps(dataShow)
	# def POST(self, length=8):
	# 	some_string = ''.join(random.sample(string.hexdigits, int(length)))
	# 	cherrypy.session['mystring'] = some_string
	# 	return some_string

	# def PUT(self, another_string):
	# 	cherrypy.session['mystring'] = another_string

	# def DELETE(self):
	# 	cherrypy.session.pop('mystring', None)


if __name__ == '__main__':
	conf = {
		'/': {
			'tools.sessions.on': True,
			'tools.staticdir.root': os.path.abspath(os.getcwd())
		},
		'/generator': {
			'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
			'tools.response_headers.on': True,
			'tools.response_headers.headers': [('Content-Type', 'text/plain')],
		},
		'/static': {
			'tools.staticdir.on': True,
			'tools.staticdir.dir': './'
		}
	}
	webapp = StringGenerator()
	webapp.generator = StringGeneratorWebService()
	cherrypy.quickstart(webapp, '/', conf)
	# cherrypy.quickstart(Root(), '/')
