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
from socket import socket, AF_INET, SOCK_DGRAM
from daemon import runner	# python-daemon


class Main():
	_gauges = {}
	_counters = {}
	def handle_data(self, raw_data):
		data = raw_data.split("|")
		if len(data)==4:
			application_tag = data[0]
			metric_name = data[1]
			metric_value = data[2]
			metric_type = data[3]
			print "TAG:%s METRIC:%s=%s [%s]" % (application_tag, metric_name, metric_value, metric_type)
		else:
			print "Invalid metric length (%d)" % (len(data))

# Global
main = Main()


class UDPStatsHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		#data = self.request.recv(1024).strip() # tcp
		raw_data = self.request[0].strip() # udp
		#socket = self.request[1]
		main.handle_data(raw_data)

class UDPListenerThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)

	def run(self):
		HOST, PORT = "127.0.0.1", 10001
		server = SocketServer.UDPServer((HOST, PORT), UDPStatsHandler)
		server.serve_forever()


class TCPStatsHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		#data = self.request.recv(1024).strip() # tcp
		raw_data = self.request[0].strip() # udp
		#socket = self.request[1]
		main.handle_data(raw_data)

class TCPListenerThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)

	def run(self):
		HOST, PORT = "127.0.0.1", 10001
		server = SocketServer.TCPServer((HOST, PORT), TCPStatsHandler)
		server.serve_forever()


class HTTPStatsHandler(BaseHTTPServer.BaseHTTPRequestHandler):
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
		#print self.path, form
		for key in form.keys():
			val = form[key].value
			print key, val
			raw_data = key+"|"+val
			main.handle_data(raw_data)
		self.send_response(200)
		self.send_header('Content-type', 'text/html')
		self.end_headers()
		self.wfile.write("HELLO")


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
	
		self._stats = {}
		self._global_lock = threading.Lock()
		self._graphite = None

	def run(self):
		listener = UDPListenerThread()
		listener.daemon = True
		listener.start()
		listener = TCPListenerThread()
		listener.daemon = True
		listener.start()
		listener = HTTPListenerThread()
		listener.daemon = True
		listener.start()
		
		while True:
			time.sleep( 10 )


if __name__ == "__main__":
	app = App()
	if sys.argv[1]=="debug":
		app.run()
	else:
		daemon_runner = runner.DaemonRunner(app)
		daemon_runner.do_action()
