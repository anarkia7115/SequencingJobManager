#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

from BaseHTTPServer import BaseHTTPRequestHandler
import cgi
import json
import sys
sys.path.insert(0, "../src")
import config

class PostHandler(BaseHTTPRequestHandler):
    
    def do_POST(self):
        self.data_string=self.rfile.read(int(self.headers['Content-Length']))
        self.send_response(200)
        self.end_headers()
        self.data_json = json.loads(self.data_string)
        print(myprint(self.data_json))
        self.wfile.write(myprint(self.data_json))
        return

def myprint(d, sb=None):
    if sb == None:
        sb = []
    else:
        pass


    for k, v in d.iteritems():
        if isinstance(v, dict):
            myprint(v, sb)
        else:
            sb.append("{0} : {1}".format(k, v))
    return '\n'.join(x for x in sb)

if __name__ == '__main__':
    from BaseHTTPServer import HTTPServer
    server = HTTPServer((config.host['request'].partition(':')[0],
                         int(config.host['request'].partition(':')[2])), 
                         PostHandler)
    print 'Starting server, use <Ctrl-C> to stop'
    server.serve_forever()
