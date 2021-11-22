#!/usr/bin/env python
import ftplib
import time
import os
import json
import urllib.request
import sys

def getIP( node ):
    ip = urllib.request.urlopen(dns + node).read()
    return str(ip.decode("utf-8") )


def download( node, files ):

    ftp = None
    size = len(files)

    connectionProblem = 0

    while len(files) > 0:

        if connectionProblem > 1:
            ftp = None
            if connectionProblem == 8:
                exit(8)
            print( "Connection refused! Try again..." )
            time.sleep(2**connectionProblem)


        if ftp is None:
            try:
                connectionProblem += 1
                ip = getIP( node )
                print( "Try to connect to", ip )
                ftp = ftplib.FTP( ip )
                ftp.login("ftp", "pythonclient")
                ftp.set_pasv(True)
                ftp.encoding='utf-8'
                print( "Connection established" )
                connectionProblem = 0
            except ConnectionRefusedError as err:
                continue
            except BaseException as err:
                print(f"Unexpected {err=}, {type(err)=}")
                continue


        filename = files[0]
        index = size - len(files) + 1
        print("Download", "[" + str( index ).rjust( len( str( size ) ) ) + "/" + str( size ) + "]" , filename)

        try:
            if os.path.exists( filename ):
                os.remove(filename)
            ftp.retrbinary( 'RETR %s' % filename, open( filename, 'wb').write, 102400)
            files.pop(0)
        except ftplib.error_perm as err:
            if( str(err) == "550 Failed to open file." ):
                print( "File not found:", node + filename )
            if exitIfFileWasNotFound:
                exit(550)
        except FileNotFoundError as err:
            print( "File not found:", node + filename )
            if exitIfFileWasNotFound:
                exit(404)
        except EOFError as err:
            print( "It seems the connection was lost! Try again..." )
            ftp = None
            continue
        except BaseException as err:
            print(f"Unexpected {err=}, {type(err)=}")
            ftp = None
            continue

        files.pop(0)

    if ftp is not None:
        try:
            ftp.quit()
            ftp.close()
        except:
            print(f"Unexpected {err=}, {type(err)=}")



print("Start to setup the environment")

exitIfFileWasNotFound = False

configFilePath = ".command.inputs.json"

if not os.path.isfile( configFilePath ):
    print ("Config file not found:", configFilePath )
    exit(102)

configFile = open(configFilePath)
config = json.load( configFile )
print( config )
dns = config["dns"]

data = config[ "data" ]

for d in data:
    files = d["files"]
    download( d["node"], files )