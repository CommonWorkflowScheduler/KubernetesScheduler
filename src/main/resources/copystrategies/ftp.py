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

exitIfFileWasNotFound = False
CLOSE = False
UNEXPECTED_ERROR = "Unexpected error"
EXIT = 0
log.basicConfig(format='%(levelname)s: %(message)s', level=log.DEBUG)


def myExit(code):
    global EXIT
    EXIT = code
    global CLOSE
    CLOSE = True
    exit(EXIT)


def close(signalnum, syncFile):
    log.info("Killed: %s", str(signalnum))
    closeWithWarning(50, syncFile)


def closeWithWarning(errorCode, syncFile):
    syncFile.write('##FAILURE##\n')
    syncFile.flush()
    syncFile.close()
    myExit(errorCode)


def getIP(node, dns):
    ip = urllib.request.urlopen(dns + node).read()
    return str(ip.decode("utf-8"))


def clearLocatation(path):
    if os.path.exists(path):
        log.debug("Delete %s", path)
        if os.path.islink(path):
            os.unlink(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)


def getFTP(node, dns, syncFile):
    connectionProblem = 0
    while connectionProblem < 8:
        try:
            ip = getIP(node, dns)
            log.info("Try to connect to %s", ip)
            ftp = ftplib.FTP(ip)
            ftp.login("ftp", "pythonclient")
            ftp.set_pasv(True)
            ftp.encoding = 'utf-8'
            log.info("Connection established")
            return ftp
        except ConnectionRefusedError:
            log.warning("Connection refused! Try again...")
        except BaseException:
            log.exception(UNEXPECTED_ERROR)
        connectionProblem += 1
        time.sleep(2 ** connectionProblem)
    closeWithWarning(8, syncFile)


def closeFTP(ftp):
    if ftp is None:
        return
    try:
        ftp.quit()
        ftp.close()
    except BaseException:
        log.exception(UNEXPECTED_ERROR)


def downloadFile(ftp, filename, size, index, node, syncFile):
    log.info("Download [%s/%s] - %s", str(index).rjust(len(str(size))), str(size), filename)
    try:
        syncFile.write("S-" + filename + '\n')
        clearLocatation(filename)
        Path(filename[:filename.rindex("/")]).mkdir(parents=True, exist_ok=True)
        ftp.retrbinary('RETR %s' % filename, open(filename, 'wb').write, 102400)
    except ftplib.error_perm as err:
        if str(err) == "550 Failed to open file.":
            log.warning("File not found node: %s file: %s", node, filename)
        if exitIfFileWasNotFound:
            closeWithWarning(40, syncFile)
    except FileNotFoundError:
        log.warning("File not found node: %s file: %s", node, filename)
        if exitIfFileWasNotFound:
            closeWithWarning(41, syncFile)
    except EOFError:
        log.warning("It seems the connection was lost! Try again...")
        return False
    except BaseException:
        log.exception(UNEXPECTED_ERROR)
        return False
    return True


def download(node, files, dns, syncFile):
    ftp = None
    size = len(files)
    global CLOSE
    while not CLOSE and len(files) > 0:
        if ftp is None:
            ftp = getFTP(node, dns, syncFile)
        filename = files[0]
        index = size - len(files) + 1
        if not downloadFile(ftp, filename, size, index, node, syncFile):
            ftp = None
            continue
        files.pop(0)
        syncFile.write("F-" + filename + '\n')
    closeFTP(ftp)


def waitForFiles(syncFilePath, files, starttime):
    # wait max. 10 seconds
    while True:
        if starttime + 10 < time.time():
            return False
        if os.path.isfile(syncFilePath):
            break
        log.debug("Wait for file creation")
        time.sleep(0.1)

    # Read file live
    with open(syncFilePath, 'r') as syncFileTask:
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
                        if text.startswith("S-"):
                            continue
                        if text == "##FAILURE##":
                            log.debug("Read FAILURE in %s", syncFilePath)
                            myExit(51)
                        if text == "##FINISHED##":
                            log.debug("Read FINISHED in " + syncFilePath + " before all files were found")
                            myExit(52)
                        log.debug("Look for " + text[:2] + " with " + text[2:] + " in " + str(files))
                        if text[:2] == "F-" and text[2:] in files:
                            files.remove(text[2:])
                            if len(files) == 0:
                                return True
    return len(files) == 0


def loadConfig(configFilePath):
    if not os.path.isfile(configFilePath):
        log.error("Config file not found: %s", configFilePath)
        myExit(102)

    with open(configFilePath, 'r') as configFile:
        config = json.load(configFile)

    log.info(str(config))

    os.makedirs(config["syncDir"], exist_ok=True)
    return config


def registerSignal(syncFile):
    signal.signal(signal.SIGINT, lambda signalnum, handler: close(signalnum, syncFile))
    signal.signal(signal.SIGTERM, lambda signalnum, handler: close(signalnum, syncFile))


def registerSignal2():
    signal.signal(signal.SIGINT, lambda signalnum, handler: myExit(1))
    signal.signal(signal.SIGTERM, lambda signalnum, handler: myExit(1))


def generateSymlinks(symlinks):
    for s in symlinks:
        src = s["src"]
        dst = s["dst"]
        clearLocatation(src)
        Path(src[:src.rindex("/")]).mkdir(parents=True, exist_ok=True)
        os.symlink(dst, src)


def downloadAllData(data, dns, syncFile):
    for d in data:
        files = d["files"]
        download(d["node"], files, dns, syncFile)


def waitForDependingTasks(waitForFilesOfTask, starttime, syncDir):
    # Now check for files of other tasks
    for waitForTask in waitForFilesOfTask:
        waitForFilesSet = set(waitForFilesOfTask[waitForTask])
        if not waitForFiles(syncDir + waitForTask, waitForFilesSet, starttime):
            log.error(syncDir + waitForTask + " was not successful")
            myExit(200)


def writeTrace(traceFilePath, dataMap):
    with open(traceFilePath, "a") as traceFile:
        for d in dataMap:
            traceFile.write(d + "=" + str(dataMap[d]))


def run():
    starttime = time.time()
    log.info("Start to setup the environment")
    config = loadConfig(".command.inputs.json")

    dns = config["dns"]
    data = config["data"]
    symlinks = config["symlinks"]

    with open(config["syncDir"] + config["hash"], 'w') as syncFile:
        registerSignal(syncFile)
        syncFile.write('##STARTED##\n')
        syncFile.flush()
        generateSymlinks(symlinks)
        syncFile.write('##SYMLINKS##\n')
        syncFile.flush()
        downloadAllData(data, dns, syncFile)
        if CLOSE:
            log.debug("Closed with code %s", str(EXIT))
            exit(EXIT)
        syncFile.write('##FINISHED##\n')
        registerSignal2()

    waitForDependingTasks(config["waitForFilesOfTask"], starttime, config["syncDir"])

    runtime = str(int((time.time() - starttime) * 1000))
    trace = {
        "scheduler_init_runtime": runtime
    }
    writeTrace(".command.scheduler.trace", trace)


if __name__ == '__main__':
    run()
