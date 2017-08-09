
from SimpleHTTPServer import SimpleHTTPRequestHandler
import BaseHTTPServer

PORT = 8083 #1968

class CORSRequestHandler (SimpleHTTPRequestHandler):
    def end_headers (self):
        self.send_header('Access-Control-Allow-Origin', '*')
        SimpleHTTPRequestHandler.end_headers(self)

if __name__ == '__main__':
    httpd = BaseHTTPServer.HTTPServer(("", PORT), CORSRequestHandler)
    httpd.serve_forever()
