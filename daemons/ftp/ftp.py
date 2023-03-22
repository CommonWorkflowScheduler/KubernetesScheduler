#!/usr/bin/env python3
import ftplib
import json
import logging as log
import os
import shutil
import signal
import sys
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import sleep

########################################################################################
# Call this class with three arguments: trace enabled, name to store logs, config json #
########################################################################################

exitIfFileWasNotFound = True
CLOSE = False
UNEXPECTED_ERROR = "Unexpected error"
EXIT = 0
log.basicConfig(
    format='%(levelname)s: %(message)s',
    level=log.DEBUG,
    handlers=[
        log.FileHandler(".command.init." + sys.argv[2] + ".log"),
        log.StreamHandler()
    ]
)
trace = {}
traceFilePath = ".command.scheduler.trace"
errors = 0


def myExit(code):
    global EXIT
    EXIT = code
    global CLOSE
    CLOSE = True
    writeTrace(trace)
    exit(EXIT)


def close(signalnum, syncFile):
    log.info("Killed: %s", str(signalnum))
    closeWithWarning(50, syncFile)


def closeWithWarning(errorCode, syncFile):
    syncFile.write('##FAILURE##\n')
    syncFile.flush()
    syncFile.close()
    myExit(errorCode)


def getIP(node, dns, execution):
    ip = urllib.request.urlopen(dns + "daemon/" + execution + "/" + node).read()
    return str(ip.decode("utf-8"))


# True if the file was deleted or did not exist
def clearLocation(path, dst=None):
    if os.path.exists(path):
        log.debug("Delete %s", path)
        if os.path.islink(path):
            if dst is not None and os.readlink(path) == dst:
                return False
            else:
                os.unlink(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
    return True


def getFTP(node, currentIP, dns, execution, syncFile):
    global errors
    connectionProblem = 0
    while connectionProblem < 8:
        try:
            if currentIP is None:
                log.info("Request ip for node: %s", node)
                ip = getIP(node, dns, execution)
            else:
                ip = currentIP
            log.info("Try to connect to %s", ip)
            ftp = ftplib.FTP(ip, timeout=10)
            ftp.login("root", "password")
            ftp.set_pasv(True)
            ftp.encoding = 'utf-8'
            log.info("Connection established")
            return ftp
        except ConnectionRefusedError:
            errors += 1
            log.warning("Connection refused! Try again...")
        except BaseException:
            errors += 1
            log.exception(UNEXPECTED_ERROR)
        connectionProblem += 1
        time.sleep(2 ** connectionProblem)
    closeWithWarning(8, syncFile)


def closeFTP(ftp):
    global errors
    if ftp is None:
        return
    try:
        ftp.quit()
        ftp.close()
    except BaseException:
        errors += 1
        log.exception(UNEXPECTED_ERROR)


def downloadFile(ftp, filename, size, index, node, syncFile, speed):
    global errors
    log.info("Download %s [%s/%s] - %s", node, str(index).rjust(len(str(size))), str(size), filename)
    try:
        syncFile.write("S-" + filename + '\n')
        clearLocation(filename)
        Path(filename[:filename.rindex("/")]).mkdir(parents=True, exist_ok=True)
        start = time.time()
        with open(filename, 'wb') as file:
            if speed == 100:
                ftp.retrbinary('RETR %s' % filename, file.write, 102400)
            else:
                timer = {"t": time.time_ns()}

                def callback(data):
                    now = time.time_ns()
                    diff = now - timer["t"]
                    file.write(data)
                    timeToSleep = (diff * (100 / speed) - diff) / 1_000_000_000
                    # sleep at least 10ms
                    if timeToSleep > 0.01:
                        time.sleep(timeToSleep)
                        timer["t"] = time.time_ns()

                ftp.retrbinary('RETR %s' % filename, callback, 102400)
        end = time.time()
        sizeInMB = os.path.getsize(filename) / 1048576
        delta = (end - start)
        log.info("Speed: %.3f Mb/s", sizeInMB / delta)
        return sizeInMB, delta
    except ftplib.error_perm as err:
        errors += 1
        if str(err) == "550 Failed to open file.":
            log.warning("File not found node: %s file: %s", node, filename)
        if exitIfFileWasNotFound:
            closeWithWarning(40, syncFile)
    except FileNotFoundError:
        errors += 1
        log.warning("File not found node: %s file: %s", node, filename)
        if exitIfFileWasNotFound:
            closeWithWarning(41, syncFile)
    except EOFError:
        errors += 1
        log.warning("It seems the connection was lost! Try again...")
        return None
    except BaseException:
        errors += 1
        log.exception(UNEXPECTED_ERROR)
        return None
    return 0, 0


def download(node, currentIP, files, dns, execution, syncFile, speed):
    ftp = None
    size = len(files)
    global CLOSE
    sizeInMB = 0
    downloadTime = 0
    while not CLOSE and len(files) > 0:
        if ftp is None:
            ftp = getFTP(node, currentIP, dns, execution, syncFile)
            currentIP = None
        filename = files[0]
        index = size - len(files) + 1
        result = downloadFile(ftp, filename, size, index, node, syncFile, speed)
        if result is None:
            ftp = None
            continue
        sizeInMB += result[0]
        downloadTime += result[1]
        files.pop(0)
        syncFile.write("F-" + filename + '\n')
    closeFTP(ftp)
    return node, sizeInMB / downloadTime


def waitForFiles(syncFilePath, files, startTime):
    # wait max. 60 seconds
    while True:
        if startTime + 60 < time.time():
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


def loadConfig():
    log.info("Load config")
    with open(sys.argv[3]) as jsonFile:
        config = json.load(jsonFile)
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
        if clearLocation(src, dst):
            Path(src[:src.rindex("/")]).mkdir(parents=True, exist_ok=True)
            try:
                os.symlink(dst, src)
            except FileExistsError:
                log.warning("File exists: %s -> %s", src, dst)


def downloadAllData(data, dns, execution, syncFile, speed):
    global trace
    throughput = []
    with ThreadPoolExecutor(max_workers=max(10, len(data))) as executor:
        futures = []
        for d in data:
            files = d["files"]
            node = d["node"]
            currentIP = d["currentIP"]
            futures.append(executor.submit(download, node, currentIP, files, dns, execution, syncFile, speed))
        lastNum = -1
        while len(futures) > 0:
            if lastNum != len(futures):
                log.info("Wait for %d threads to finish", len(futures))
                lastNum = len(futures)
            for f in futures[:]:
                if f.done():
                    throughput.append(f.result())
                    futures.remove(f)
            sleep(0.1)
    trace["scheduler_init_throughput"] = "\"" + ",".join("{}:{:.3f}".format(*x) for x in throughput) + "\""


def waitForDependingTasks(waitForFilesOfTask, startTime, syncDir):
    # Now check for files of other tasks
    for waitForTask in waitForFilesOfTask:
        waitForFilesSet = set(waitForFilesOfTask[waitForTask])
        if not waitForFiles(syncDir + waitForTask, waitForFilesSet, startTime):
            log.error(syncDir + waitForTask + " was not successful")
            myExit(200)


def writeTrace(dataMap):
    if sys.argv[1] == 'true':
        global errors
        if len(dataMap) == 0 or errors > 0:
            return
        with open(traceFilePath, "a") as traceFile:
            for d in dataMap:
                traceFile.write(d + "=" + str(dataMap[d]) + "\n")
            traceFile.write("scheduler_init_errors=" + str(errors) + "\n")


def finishedDownload(dns, execution, taskname):
    try:
        dns = dns + "downloadtask/" + execution
        log.info("Request: %s", dns)
        urllib.request.urlopen(dns, taskname.encode("utf-8"))
    except BaseException as err:
        log.exception(err)
        myExit(100)


def run():
    global trace
    startTime = time.time()
    log.info("Start to setup the environment")
    config = loadConfig()

    dns = config["dns"]
    execution = config["execution"]
    data = config["data"]
    symlinks = config["symlinks"]
    taskname = config["hash"]

    with open(config["syncDir"] + config["hash"], 'w') as syncFile:
        registerSignal(syncFile)
        syncFile.write('##STARTED##\n')
        syncFile.flush()
        startTimeSymlinks = time.time()
        generateSymlinks(symlinks)
        trace["scheduler_init_symlinks_runtime"] = int((time.time() - startTimeSymlinks) * 1000)
        syncFile.write('##SYMLINKS##\n')
        syncFile.flush()
        startTimeDownload = time.time()
        downloadAllData(data, dns, execution, syncFile, config["speed"])
        trace["scheduler_init_download_runtime"] = int((time.time() - startTimeDownload) * 1000)
        if CLOSE:
            log.debug("Closed with code %s", str(EXIT))
            exit(EXIT)
        log.info("Finished Download")
        syncFile.write('##FINISHED##\n')
        registerSignal2()

    # finishedDownload(dns, execution, taskname)

    # startTimeDependingTasks = time.time()
    # waitForDependingTasks(config["waitForFilesOfTask"], startTime, config["syncDir"])
    # trace["scheduler_init_depending_tasks_runtime"] = int((time.time() - startTimeDependingTasks) * 1000)
    # log.info("Waited for all tasks")

    # runtime = int((time.time() - startTime) * 1000)
    # trace["scheduler_init_runtime"] = runtime
    # writeTrace(trace)


if __name__ == '__main__':
    run()
