#!/usr/bin/env python
import ftplib
import time
import os
import json
import urllib.request
from pathlib import Path
import shutil
import signal
import logging as log

CLOSE = False
EXIT = 0
log.basicConfig(format='%(levelname)s: %(message)s', level=log.DEBUG)

def myExit( code ):
    global EXIT
    EXIT = code
    global CLOSE
    CLOSE = True
    exit(EXIT)

def close( signalnum, syncfile ):
    syncFile.write('##FAILURE##\n')
    syncFile.flush()
    syncFile.close()
    log.info( "Killed: %s",str(signalnum))
    myExit(50)

def getIP( node ):
    ip = urllib.request.urlopen(dns + node).read()
    return str(ip.decode("utf-8") )

def clearLocatation( path ):
    if os.path.exists( path ):
        log.debug( "Delete %s",path )
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
    global CLOSE

    while not CLOSE and len(files) > 0:

        if connectionProblem > 1:
            ftp = None
            if connectionProblem == 8:
                myExit( 8 )
            log.warning(  "Connection refused! Try again..." )
            time.sleep(2**connectionProblem)

        if ftp is None:
            try:
                connectionProblem += 1
                ip = getIP( node )
                log.info( "Try to connect to %s", ip )
                ftp = ftplib.FTP( ip )
                ftp.login("ftp", "pythonclient")
                ftp.set_pasv(True)
                ftp.encoding='utf-8'
                log.info( "Connection established" )
                connectionProblem = 0
            except ConnectionRefusedError as err:
                continue
            except BaseException as err:
                log.exception( "Unexpected error" )
                continue


        filename = files[0]
        index = size - len(files) + 1
        log.info( "Download [%s/%s] - %s", str( index ).rjust( len( str( size ) ) ), str( size ), filename )

        try:
            syncFile.write("S-" + filename + '\n')
            clearLocatation( filename )
            Path(filename[:filename.rindex("/")]).mkdir(parents=True, exist_ok=True)
            ftp.retrbinary( 'RETR %s' % filename, open( filename, 'wb').write, 102400)
        except ftplib.error_perm as err:
            if( str(err) == "550 Failed to open file." ):
                log.warning(  "File not found node: %s file: %s", node, filename )
            if exitIfFileWasNotFound:
                myExit( 40 )
        except FileNotFoundError as err:
            log.warning( "File not found node: %s file: %s", node, filename )
            if exitIfFileWasNotFound:
                myExit( 41 )
        except EOFError as err:
            log.warning( "It seems the connection was lost! Try again..." )
            ftp = None
            continue
        except BaseException as err:
            log.exception( "Unexpected error" )
            ftp = None
            continue

        files.pop(0)
        syncFile.write("F-" + filename + '\n')

    if ftp is not None:
        try:
            ftp.quit()
            ftp.close()
        except BaseException as err:
            log.exception( "Unexpected error" )

def waitForFiles( syncFileTask, files, starttime ):
    #wait max. 10 seconds
    while True:
        if starttime + 10 < time.time():
            return False
        if os.path.isfile( syncFileTask ):
            break
        log.debug( "Wait for file creation" )
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
                        current = []
                        if text.startswith( "S-" ):
                            continue
                        if text == "##FAILURE##":
                            log.debug( "Read FAILURE in %s", syncFileTask )
                            myExit( 51 )
                        if text == "##FINISHED##":
                            log.debug( "Read FINISHED in " + syncFileTask + " before all files were found" )
                            myExit( 52 )
                        log.debug( "Look for " + text[:2] + " with " + text[2:] + " in " + str(files) )
                        if text[:2] == "F-" and text[2:] in files:
                            files.remove( text[2:] )
                            if len(files) == 0:
                                return True
    return len(files) == 0

starttime = time.time()
log.info( "Start to setup the environment" )

exitIfFileWasNotFound = False

configFilePath = ".command.inputs.json"

if not os.path.isfile( configFilePath ):
    log.error( "Config file not found:", configFilePath )
    myExit( 102 )

with open(configFilePath,'r') as configFile:
    config = json.load( configFile )

log.info( str(config) )
dns = config["dns"]

data = config[ "data" ]
symlinks = config[ "symlinks" ]

os.makedirs( config[ "syncDir" ], exist_ok=True)
with open( config[ "syncDir" ] + config[ "hash" ], 'w' ) as syncFile:

    signal.signal( signal.SIGINT, lambda signalnum, handler: close( signalnum, syncFile ) )
    signal.signal( signal.SIGTERM, lambda signalnum, handler: close( signalnum, syncFile ) )

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

    if CLOSE:
        log.debug( "Close with code %s", str(EXIT) )
        exit( EXIT )

    syncFile.write('##FINISHED##\n')

    signal.signal( signal.SIGINT, lambda signalnum, handler: myExit( 1 ) )
    signal.signal( signal.SIGTERM, lambda signalnum, handler: myExit( 1 ) )

#No check for files of other tasks
for waitForTask in config[ "waitForFilesOfTask" ]:
    waitForFilesSet = set( config[ "waitForFilesOfTask" ][ waitForTask ] )
    if not waitForFiles( config[ "syncDir" ] + waitForTask, waitForFilesSet, starttime ):
        log.error( config[ "syncDir" ] + waitForTask + " was not successful" )
        myExit( 200 )

traceFilePath = ".command.scheduler.trace"

with open(traceFilePath, "a") as traceFile:
    traceFile.write("scheduler_init_runtime=" + str(int((time.time()-starttime)*1000)))