#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

from BaseHTTPServer import BaseHTTPRequestHandler
import cgi
import json

class PostHandler(BaseHTTPRequestHandler):
    
    def do_POST(self):
        """
        # Parse the form data posted
        form = cgi.FieldStorage(
            fp=self.rfile, 
            headers=self.headers,
            environ={'REQUEST_METHOD':'POST',
                     'CONTENT_TYPE':self.headers['Content-Type'],
                     })
        print "form:{}".format( form)

        # Begin the response
        self.send_response(200)
        self.end_headers()
        self.wfile.write('Client: %s\n' % str(self.client_address))
        self.wfile.write('User-agent: %s\n' % str(self.headers['user-agent']))
        self.wfile.write('Path: %s\n' % self.path)
        self.wfile.write('Form data:\n')

        # Echo back information about what was posted in the form
        for field in form.keys():
            field_item = form[field]
            if field_item.filename:
                # The field contains an uploaded file
                file_data = field_item.file.read()
                file_len = len(file_data)
                del file_data
                self.wfile.write('\tUploaded %s as "%s" (%d bytes)\n' % \
                        (field, field_item.filename, file_len))
            else:
                # Regular form value
                self.wfile.write('\t%s=%s\n' % (field, form[field].value))
        """
        self.data_string=self.rfile.read(int(self.headers['Content-Length']))
        self.send_response(200)
        self.end_headers()
        self.data_json = json.loads(self.data_string)
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
    server = HTTPServer(('192.168.2.156', 8080), PostHandler)
    print 'Starting server, use <Ctrl-C> to stop'
    server.serve_forever()
