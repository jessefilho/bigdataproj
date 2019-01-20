#!/usr/bin/env python

'''
Simple and functional REST server for Python (2.7) using no dependencies beyond the Python standard library.
Features:
* Map URI patterns using regular expressions
* Map any/all the HTTP VERBS (GET, PUT, DELETE, POST)
* All responses and payloads are converted to/from JSON for you
* Easily serve static files: a URI can be mapped to a file, in which case just GET is supported
* You decide the media type (text/html, application/json, etc.)
* Correct HTTP response codes and basic error messages
* Simple REST client included! use the rest_call_json() method
As an example, let's support a simple key/value store. To test from the command line using curl:
curl "http://localhost:8080/records"
curl -X PUT -d '{"name": "Tal"}' "http://localhost:8080/record/1"
curl -X PUT -d '{"name": "Shiri"}' "http://localhost:8080/record/2"
curl "http://localhost:8080/records"
curl -X DELETE "http://localhost:8080/record/2"
curl "http://localhost:8080/records"
Create the file web/index.html if you'd like to test serving static files. It will be served from the root URI.
@author: Tal Liron (tliron @ github.com)
'''

import sys, os, re, shutil, json, urllib, urllib2, BaseHTTPServer
import bdproj

# Fix issues with decoding HTTP responses
reload(sys)
sys.setdefaultencoding('utf8')



here = os.path.dirname(os.path.realpath(__file__))

records = {}


#Question1
def get_question1(handler):
    print len(handler.path)

    id_student = urllib.unquote(handler.path[20:-15]) #2017000304
    program = urllib.unquote(handler.path[43:])

    return bdproj.question1(id_student,program)

#Question2
def get_question2(handler):
    print len(handler.path)
    key = urllib.unquote(handler.path[17:])
    print key
    return bdproj.question2(key)

#Question3
def get_question3(handler):
    print len(handler.path)
    key = urllib.unquote(handler.path[19:-11])
    print key
    return bdproj.question3(key)

#Question4
def get_question4(handler):
    print len(handler.path)
    ue = urllib.unquote(handler.path[19:-11])
    print ue
    year =  urllib.unquote(handler.path[33:])
    print year
    return bdproj.question4(ue,year)

#Question5
def get_question5(handler):
    print len(handler.path)
    program = urllib.unquote(handler.path[19:-11])
    print program
    year =  urllib.unquote(handler.path[33:])
    print year
    return bdproj.question5(program, year)

#Question6
def get_question6(handler):
    print len(handler.path)
    name_prof = urllib.unquote(handler.path[23:-6])
    print name_prof

    return bdproj.question6(name_prof)

#Question7
def get_question7(handler):
    print len(handler.path)
    program = urllib.unquote(handler.path[19:-11])
    print program
    year =  urllib.unquote(handler.path[33:])
    print year
    return bdproj.question5(program, year)

def get_records(handler):
    return records


def get_record(handler):
    key = urllib.unquote(handler.path[8:])
    print key
    return records[key] if key in records else None


def set_record(handler):
    key = urllib.unquote(handler.path[8:])
    payload = handler.get_payload()
    records[key] = payload
    return records[key]


def delete_record(handler):
    key = urllib.unquote(handler.path[8:])
    del records[key]
    return True  # anything except None shows success


def rest_call_json(url, payload=None, with_payload_method='PUT'):
    'REST call with JSON decoding of the response and JSON payloads'
    if payload:
        if not isinstance(payload, basestring):
            payload = json.dumps(payload)
        # PUT or POST
        response = urllib2.urlopen(
            MethodRequest(url, payload, {'Content-Type': 'application/json'}, method=with_payload_method))
    else:
        # GET
        response = urllib2.urlopen(url)
    response = response.read().decode()
    return json.loads(response)


class MethodRequest(urllib2.Request):
    'See: https://gist.github.com/logic/2715756'

    def __init__(self, *args, **kwargs):
        if 'method' in kwargs:
            self._method = kwargs['method']
            del kwargs['method']
        else:
            self._method = None
        return urllib2.Request.__init__(self, *args, **kwargs)

    def get_method(self, *args, **kwargs):
        return self._method if self._method is not None else urllib2.Request.get_method(self, *args, **kwargs)


class RESTRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.routes = {
            r'^/$': {'file': 'web/index.html', 'media_type': 'text/html'},
            r'^/records$': {'GET': get_records, 'media_type': 'application/json'},

            #Big Data Projet URI /Aiwsbu/v1/courses/{id}/rates/{year}

            r'^/aiwsbu/v1/students/([^/])+/transcripts/([^/])+$': {'GET': get_question1, 'media_type': 'application/json'},
            r'^/aiwsbu/v1/rates/([^/])+$': {'GET': get_question2, 'media_type': 'application/json'},
            r'^/aiwsbu/v1/courses/([^/])+/rates$': {'GET': get_question3, 'media_type': 'application/json'},
            r'^/aiwsbu/v1/courses/([^/])+/rates/([^/])+$': {'GET': get_question4,'media_type': 'application/json'},
            r'^/aiwsbu/v1/programs/([^/])+/means/([^/])+$': {'GET': get_question5, 'media_type': 'application/json'},
            r'^/aiwsbu/v1/instructors/([^/])+/rates$': {'GET': get_question6, 'media_type': 'application/json'},
            r'^/aiwsbu/v1/ranks/([^/])+/years/([^/])+$': {'GET': get_question7, 'media_type': 'application/json'},

            r'^/record/': {'GET': get_record, 'PUT': set_record, 'DELETE': delete_record,
                           'media_type': 'application/json'}}

        return BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def do_HEAD(self):
        self.handle_method('HEAD')

    def do_GET(self):
        self.handle_method('GET')

    def do_POST(self):
        self.handle_method('POST')

    def do_PUT(self):
        self.handle_method('PUT')

    def do_DELETE(self):
        self.handle_method('DELETE')

    def get_payload(self):
        payload_len = int(self.headers.getheader('content-length', 0))
        payload = self.rfile.read(payload_len)
        payload = json.loads(payload)
        return payload

    def handle_method(self, method):
        route = self.get_route()
        try:
            if route is None:
                self.send_response(404)
                self.end_headers()
                self.wfile.write('Route not found\n')
            else:
                if method == 'HEAD':
                    self.send_response(200)
                    if 'media_type' in route:
                        self.send_header('Content-type', route['media_type'])
                    self.end_headers()
                else:
                    if 'file' in route:
                        if method == 'GET':
                            try:
                                f = open(os.path.join(here, route['file']))
                                try:
                                    self.send_response(200)
                                    if 'media_type' in route:
                                        self.send_header('Content-type', route['media_type'])
                                    self.end_headers()
                                    shutil.copyfileobj(f, self.wfile)
                                finally:
                                    f.close()
                            except:
                                self.send_response(404)
                                self.end_headers()
                                self.wfile.write('File not found\n')
                        else:
                            self.send_response(405)
                            self.end_headers()
                            self.wfile.write('Only GET is supported\n')
                    else:
                        if method in route:
                            content = route[method](self)
                            if content is not None:
                                self.send_response(200)
                                if 'media_type' in route:
                                    self.send_header('Content-type', route['media_type'])
                                self.end_headers()
                                if method != 'DELETE':
                                    self.wfile.write(content) #old (json.dumps(content))
                            else:
                                self.send_response(404)
                                self.end_headers()
                                self.wfile.write('Not found\n')
                        else:
                            self.send_response(405)
                            self.end_headers()
                            self.wfile.write(method + ' is not supported\n')
        except ValueError:
            self.send_response(404)
            self.end_headers()
            self.wfile.write('Not found\n')

    def get_route(self):
        print self
        for path, route in self.routes.iteritems():
            print path
            print route
            if re.match(path, self.path):
                return route
        return None


def rest_server(port):
    'Starts the REST server'
    http_server = BaseHTTPServer.HTTPServer(('', port), RESTRequestHandler)
    print 'Starting HTTP server at port %d' % port
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        pass
    print 'Stopping HTTP server'
    http_server.server_close()


def main(argv):
    rest_server(8080)


if __name__ == '__main__':
    main(sys.argv[1:])