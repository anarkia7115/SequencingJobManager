#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

from BaseHTTPServer import BaseHTTPRequestHandler
import cgi
import json

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
    server = HTTPServer(('192.168.2.156', 8081), PostHandler)
    print 'Starting server, use <Ctrl-C> to stop'
    server.serve_forever()
