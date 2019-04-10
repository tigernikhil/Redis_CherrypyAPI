import zipfile, urllib.request, shutil
import datetime, pdb
from datetime import timedelta
import sys, os
import fileinput
import csv, redis, json
import sys, cherrypy
import random
from redis import StrictRedis
from io import BytesIO, TextIOWrapper
import multiprocessing as mp
import itertools
import time, ssl


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
#REDIS_HOST = 'zerodhaapp.herokuapp.com'
REDIS_HOST = os.environ.get('REDISCLOUD_URL')

def read_csv_data(csv_file, ik, iv):
	with open(csv_file, encoding='utf-8') as csvf:
		csv_data = csv.reader(csvf)
		return [(r[ik], r[iv]) for r in csv_data]

def store_data(conn, data):
	for i in data:
		conn.setnx(i[0], i[1])
	return data

def find_bad_qn(a, url, file_name, fileName):
	try:
		# pdb.set_trace()
		# redis_host = os.getenv('REDIS_URL', 'localhost')
		conn = redis.StrictRedis(REDIS_HOST, charset="utf-8", decode_responses=True)
		# context = ssl._create_unverified_context()
		# resp = urllib.request.urlopen(url, context=context)
		context = ssl._create_unverified_context()
		with urllib.request.urlopen(url, context=context) as response, open(file_name, 'wb') as out_file:
			shutil.copyfileobj(response, out_file)
			# pdb.set_trace()
			with zipfile.ZipFile(file_name) as zf:
				zf.extractall()
		# zipfile1 = zipfile.ZipFile(BytesIO(resp.read()))
		# tems_file  = zipfile1.open(fileName)

		# # Read CSV
		# items_file  = TextIOWrapper(items_file, encoding='UTF-8', newline='')
		# cr = csv.DictReader(items_file)
		# conn = redis.Redis(REDIS_HOST)
		count = 1
	# with open(fileName) as csv_file:
	# 	csv_reader = csv.reader(csv_file, delimiter=',')
	# 	line_count = 0
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

		# ////we commented this code

		# context = ssl._create_unverified_context()
		# with urllib.request.urlopen(url, context=context) as response, open(file_name, 'wb') as out_file:
		# 	shutil.copyfileobj(response, out_file)
		# 	# pdb.set_trace()
		# 	with zipfile.ZipFile(file_name) as zf:
		# 		zf.extractall()
		os.remove(fileName)
		os.remove(file_name)
		return 0
	except:
		return 1

def main():
	now = datetime.datetime.now() - timedelta(1)
	day = '%02d' % now.day
	month = '%02d' % now.month
	year = '%02d' % now.year
	fileName = 'EQ' + day + month + year[2:4] + '.csv'
	# pdb.set_trace()
	url = 'https://www.bseindia.com/download/BhavCopy/Equity/EQ'+ day + month + year[2:4] +'_CSV.ZIP'
	file_name = 'myzip.zip'
	countx = 0
	for i in range(298314,298346):
		check = find_bad_qn(i, url, file_name, fileName)
		if check == 1:
			countx += 1
			if countx % 10 == 0:
				now = now - timedelta(1)
				day = '%02d' % now.day
				month = '%02d' % now.month
				year = '%02d' % now.year
				fileName = 'EQ' + day + month + year[2:4] + '.csv'
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


	# ////we commented this code


	# conn = redis.Redis(REDIS_HOST)
	# count = 1
	# with open(fileName) as csv_file:
	# 	csv_reader = csv.reader(csv_file, delimiter=',')
	# 	line_count = 0
	# 	for row in csv_reader:
			
	# 		if count == 1:
	# 			count += 1
	# 			header = row
	# 		else:
	# 			hashKey = row[1].upper()
				
	# 			hashKey = hashKey.replace(" ", "")
				
	# 			index = 0
	# 			for head in header:
	# 				conn.hset(hashKey, head, row[index])
	# 				index += 1
				
	# os.remove(fileName)
	# os.remove(file_name)
		
class StringGenerator:
	@cherrypy.expose
	def index(self):
		main()
		return open('index.html')

class StringGeneratorWebService(object):
	exposed = True

	@cherrypy.tools.accept(media='text/plain')
	def GET(self):
		conn = redis.StrictRedis.from_url(REDIS_HOST, charset="utf-8", decode_responses=True)
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

	@cherrypy.tools.accept(media='text/plain')
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
		'global': {
			'server.socket_host':  '0.0.0.0',
			'server.socket_port':  int(os.environ.get('PORT', '8000'))
		},
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
