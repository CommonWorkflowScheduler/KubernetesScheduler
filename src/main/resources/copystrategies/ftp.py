#!/usr/bin/env python
import ftplib
import time
import os
import json
import urllib.request
import sys
from pathlib import Path
import shutil

def getIP( node ):
    ip = urllib.request.urlopen(dns + node).read()
    return str(ip.decode("utf-8") )

def clearLocatation( path ):
    if os.path.exists( path ):
        print("Delete",path)
        if os.path.islink( path ):
            os.unlink( path )
        elif os.path.isdir( path ):
            shutil.rmtree( path )
        else:
            os.remove( path )


def download( node, files, syncFile ):

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
            clearLocatation( filename )
            Path(filename[:filename.rindex("/")]).mkdir(parents=True, exist_ok=True)
            ftp.retrbinary( 'RETR %s' % filename, open( filename, 'wb').write, 102400)
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
        syncFile.write(filename + '\n')

    if ftp is not None:
        try:
            ftp.quit()
            ftp.close()
        except BaseException as err:
            print(f"Unexpected {err=}, {type(err)=}")

def waitForFiles( syncFileTask, files, starttime ):
    #wait max. 10 seconds
    while True:
        if starttime + 10 < time.time():
            return False
        if os.path.isfile( syncFileTask ):
            break
        print("Wait for file creation")
        time.sleep(0.1)

    #Read file live
    with open( syncFileTask, 'r' ) as syncFileTask:
        current = []
        while len(files) > 0:
            data = syncFileTask.read()
            if not data:
                time.sleep(0.3)
            else:
                for d in data:
                    if d != "\n":
                        current.append(d)
                    else:
                        text = ''.join(current)
                        if text == "##FAILURE##":
                            print("Read FAILURE in " + syncFileTask)
                            sys.exit(1001)
                        if text == "##FINISHED##":
                            print("Read FINISHED in " + syncFileTask + " before all files were found")
                            sys.exit(1002)
                        if text in files:
                            files.remove( text )
                            if len(files) == 0:
                                return True
                        current = []
    return True

starttime = time.time()
print("Start to setup the environment")

exitIfFileWasNotFound = False

configFilePath = ".command.inputs.json"

if not os.path.isfile( configFilePath ):
    print ("Config file not found:", configFilePath )
    exit(102)

with open(configFilePath,'r') as configFile:
    config = json.load( configFile )

print( config )
dns = config["dns"]

data = config[ "data" ]
symlinks = config[ "symlinks" ]

os.makedirs( config[ "syncDir" ], exist_ok=True)
with open( config[ "syncDir" ] + config[ "hash" ], 'w' ) as syncFile:

    syncFile.write('##STARTED##\n')
    syncFile.flush()

    for s in symlinks:
        src = s["src"]
        dst = s["dst"]
        clearLocatation( src )
        Path(src[:src.rindex("/")]).mkdir(parents=True, exist_ok=True)
        os.symlink( dst, src )

    syncFile.write('##SYMLINKS##\n')

    for d in data:
        files = d["files"]
        download( d["node"], files, syncFile )

    syncFile.write('##FINISHED##\n')

#No check for files of other tasks
for waitForTask in config[ "waitForFilesOfTask" ]:
    waitForFilesSet = set( config[ "waitForFilesOfTask" ][ waitForTask ] )
    if not waitForFiles( config[ "syncDir" ] + waitForTask, waitForFilesSet, starttime ):
        sys.exit(2000)
