#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
 * stats_daemon
 * Copyright (C) 2009-2012 Adam Etienne <etienne.adam@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation version 3.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''

import os
import sys
import time
import threading
import SocketServer
import BaseHTTPServer
import cgi
import hashlib
from socket import socket, AF_INET, SOCK_DGRAM
from daemon import runner	# python-daemon


class Access():
	def __init__(self):
		self._server = "localhost"
		self._port = 80

	def auth_ip(self, user_uuid, ip):
		# Call REST API to get allowed IPs
		valid_ips = ['82.216.246.132']
		return ip in valid_ips

	def auth_origin(self, user_uuid, origin, token):
		# Call REST API to get clear_token
		valid_token = '62fbaa0c29bfe2c5b85444b53419e39ebb896e58'
		hash_token = token+'HYPERION'
		for i in xrange(0,100):
			hash_token = hashlib.sha1(hash_token).hexdigest()
		#print hash_token
		if hash_token==valid_token:
			return True
		return False
	
	def auth(self, user_uuid, auth_data):
		if not ( 'ip' in auth_data or 'origin' in auth_data ):
			# No auth method available
			return False
		auth_res = True
		if 'ip' in auth_data:
			auth_res = auth_res and self.auth_ip(user_uuid, auth_data['ip'])
		if 'origin' in auth_data:
			auth_res = auth_res and self.auth_origin(user_uuid, auth_data['origin'], auth_data['token'])
		return auth_res



class GraphiteClient():
	def __init__(self, server="localhost", port=2003):
		self._server = server
		self._port = port

	def send(self, data):
		sock = socket()
		try:
			sock.connect( (self._server, self._port) )
		except:
			print "Couldn't connect to %(server)s on port %(port)d, is carbon-agent.py running?" % { 'server':self._server, 'port':self._port }
			return False
		lines = []
		timestamp = time.time()
		for metric in data:
			#metric = data[d]
			lines.append( "%s %s %d" % (metric[0], metric[1], timestamp) )
		message = '\n'.join(lines) + '\n'
		#print message
		sock.sendall(message)


class TaskThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self._finished = threading.Event()
		self._interval = 5.0

	def setInterval(self, interval):
		self._interval = interval
	
	def shutdown(self):
		self._finished.set()

	def run(self):
		#time.sleep(2)
		while 1:
			if self._finished.isSet(): return
			self.task()
			self._finished.wait(self._interval)

	def task(self):
		#print "PUSH & CLEAR"
		#print main.gauges
		#print main.averages
		for metric_name, metric_value in main.gauges.items():
			main.gc.send([[metric_name, metric_value]])
		for metric_name, metric_value in main.averages.items():
			main.gc.send([[metric_name, avg(metric_value)]])
		main.gauges = {}
		main.averages = {}
		main.counters = {}


class Main():
	gauges = {}
	averages = {}
	counters = {}
	def __init__(self):
		self.gc = GraphiteClient("graphite.lunasys.fr")

	def handle_data(self, raw_data, auth_data):
		data = raw_data.split(":")
		#print data
		a = Access()
		if len(data)==2 and len(data[1].split("|"))==2:
			metric_name = data[0]
			data2 = data[1].split("|")
			metric_value = data2[0]
			metric_type = data2[1]
			metric_names = metric_name.split(".")
			user_uuid = metric_names[0]
			if not a.auth(user_uuid, auth_data):
				print "Auth error"
				return False
			#print "METRIC:%s=%s [%s]" % (metric_name, metric_value, metric_type)
			if metric_type=="r":
				#print "[RAW] METRIC:%s=%s [%s]" % (metric_name, metric_value, metric_type)
				self.gc.send([[metric_name, metric_value]])
			elif metric_type=="g":
				#print "[GAUGE] METRIC:%s=%s [%s]" % (metric_name, metric_value, metric_type)
				self.gauges[metric_name] = metric_value
			elif metric_type=="a":
				#print "[GAUGE] METRIC:%s=%s [%s]" % (metric_name, metric_value, metric_type)
				if not metric_name in self.gauges:
					self.averages[metric_name] = []
				self.averages[metric_name].append( metric_value )
			elif metric_type=="c":
				#print "[GAUGE] METRIC:%s=%s [%s]" % (metric_name, metric_value, metric_type)
				if not metric_name in self.gauges:
					self.counters[metric_name] = 0
				self.counters[metric_name]+= metric_value
		else:
			print "Invalid metric length (%d)" % (len(data))

# Global
main = Main()


# UDP
class UDPStatsHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		ip = self.client_address[0]
		auth_data = {'ip': ip}
		#data = self.request.recv(1024).strip() # tcp
		raw_data = self.request[0].strip() # udp
		#socket = self.request[1]
		main.handle_data(raw_data, auth_data)

class UDPListenerThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)

	def run(self):
		HOST, PORT = "0.0.0.0", 10001
		server = SocketServer.UDPServer((HOST, PORT), UDPStatsHandler)
		server.serve_forever()


# TCP
class TCPStatsHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		ip = self.client_address[0]
		auth_data = {'ip': ip}
		#data = self.request.recv(1024).strip() # tcp
		raw_data = self.request[0].strip() # udp
		#socket = self.request[1]
		main.handle_data(raw_data, auth_data)

class TCPListenerThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)

	def run(self):
		HOST, PORT = "0.0.0.0", 10001
		server = SocketServer.TCPServer((HOST, PORT), TCPStatsHandler)
		server.serve_forever()


# HTTP
class HTTPStatsHandler(BaseHTTPServer.BaseHTTPRequestHandler):
	def log_message(self, format, *args):
		return

	def do_GET(self):
		self.send_response(200)
		self.send_header('Content-type', 'text/html')
		self.end_headers()
		self.wfile.write("Nothing to see here")

	def do_POST(self):
		form = cgi.FieldStorage(
			fp=self.rfile,
			headers=self.headers,
			environ={'REQUEST_METHOD':'POST',
					'CONTENT_TYPE':self.headers['Content-Type'],
			})
		#print self.headers

		auth_data = {}
		head_origin = ""
		for k, v in self.headers.items():
			if k.strip()=='origin':
				head_origin = str(v)
		auth_data['origin'] = head_origin

		error = False
		if head_origin=="":
			error = "origin"
		else:
			for key in form.keys():
				val = form[key].value
				if key=='m':
					if 'token' in auth_data:
						raw_data = val
						main.handle_data(raw_data, auth_data)
					else:
						error = 'token'
				elif key=='auth':
					auth_token = val
					auth_data['token'] = auth_token
		#print error
		self.send_response(500 if error else 200)
		self.send_header('Access-Control-Allow-Origin', '*')
		self.send_header('Content-type', 'text/html')
		self.end_headers()
		self.wfile.write(error if error else "OK")


class HTTPListenerThread(threading.Thread):
	def run(self):
		server_address = ('', 10002)
		httpd = BaseHTTPServer.HTTPServer(server_address, HTTPStatsHandler)
		httpd.serve_forever()



class App():
	def __init__(self):
		self.stdin_path = '/dev/null'
		self.stdout_path = '/dev/tty'
		self.stderr_path = '/dev/tty'
		self.pidfile_path = '/tmp/foo.pid'
		self.pidfile_timeout = 5

		if sys.argv[1]=="restart" and not os.path.exists(self.pidfile_path):
			print "Not running."
			sys.exit(0)

		self._stats = {}
		self._global_lock = threading.Lock()
		self._graphite = None

	def run(self):
		time.sleep(2)
		listener = UDPListenerThread()
		listener.daemon = True
		listener.start()
		listener = TCPListenerThread()
		listener.daemon = True
		listener.start()
		listener = HTTPListenerThread()
		listener.daemon = True
		listener.start()

		task = TaskThread()
		task.daemon = True
		task.start()
		
		while True:
			time.sleep( 10 )


if __name__ == "__main__":
	app = App()
	if sys.argv[1]=="debug":
		app.run()
	else:
		daemon_runner = runner.DaemonRunner(app)
		daemon_runner.do_action()
