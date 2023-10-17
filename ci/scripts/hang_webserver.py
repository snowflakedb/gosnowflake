#!/usr/bin/env python3
import sys
from http.server import BaseHTTPRequestHandler,HTTPServer
from socketserver import ThreadingMixIn
import threading
import time
import json

class HTTPRequestHandler(BaseHTTPRequestHandler):
    invocations = 0

    def do_POST(self):
        if self.path.startswith('/reset'):
            print("Resetting HTTP mocks")
            HTTPRequestHandler.invocations = 0
            self.__respond(200)
        elif self.path.startswith('/invocations'):
            self.__respond(200, body=str(HTTPRequestHandler.invocations))
        elif self.path.startswith('/ocsp'):
            print("ocsp")
            self.ocspMocks()
        elif self.path.startswith('/session/v1/login-request'):
            self.authMocks()

    def ocspMocks(self):
        if self.path.startswith('/ocsp/403'):
            self.send_response(403)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
        elif self.path.startswith('/ocsp/404'):
            self.send_response(404)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
        elif self.path.startswith('/ocsp/hang'):
            print("Hanging")
            time.sleep(300)
            self.send_response(200, 'OK')
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
        else:
            self.send_response(200, 'OK')
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()

    def authMocks(self):
        content_length = int(self.headers.get('content-length', 0))
        body = self.rfile.read(content_length)
        jsonBody = json.loads(body)
        if jsonBody['data']['ACCOUNT_NAME'] == "jwtAuthTokenTimeout":
            HTTPRequestHandler.invocations += 1
            if HTTPRequestHandler.invocations >= 3:
                self.__respond(200, body='''{
                    "data": {
                        "token": "someToken"
                    },
                    "success": true
                }''')
            else:
                time.sleep(2000)
                self.send_response(200)
        else:
            print("Unknown auth request")
            self.send_response(500)

    def __respond(self, http_code, content_type='application/json', body=None):
        print("responding:", body)
        self.send_response(http_code)
        self.send_header('Content-Type', content_type)
        self.end_headers()
        if body != None:
            responseBody = bytes(body, "utf-8")
            self.wfile.write(responseBody)

    do_GET = do_POST

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
  allow_reuse_address = True

  def shutdown(self):
    self.socket.close()
    HTTPServer.shutdown(self)

class SimpleHttpServer():
  def __init__(self, ip, port):
    self.server = ThreadedHTTPServer((ip,port), HTTPRequestHandler)

  def start(self):
    self.server_thread = threading.Thread(target=self.server.serve_forever)
    self.server_thread.daemon = True
    self.server_thread.start()

  def waitForThread(self):
    self.server_thread.join()

  def stop(self):
    self.server.shutdown()
    self.waitForThread()

if __name__=='__main__':
    if len(sys.argv) != 2:
        print("Usage: python3 {} PORT".format(sys.argv[0]))
        sys.exit(2)

    PORT = int(sys.argv[1])

    server = SimpleHttpServer('localhost', PORT)
    print('HTTP Server Running on PORT {}..........'.format(PORT))
    server.start()
    server.waitForThread()

