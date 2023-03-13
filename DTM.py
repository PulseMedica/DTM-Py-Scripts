## Data Transfer Manager
## written 3-10-2023
## 
## A collection of functions to backup and transfer
## files from local to NAS to cloud storage, to be
## imported and executed by scheduled tasks.
## ====================================================

from datetime import datetime
from dateutil import tz
import lzma
import mmap
import os
from pathlib import Path
import platform
import random
import shutil
import string
import subprocess
import sys
import time

# CONSTANTS===============
startUploadTime = 1800; # Hours for uploading: 8 PM - 6 AM
endUploadTime = 600;
OStype = platform.system(); # will be one of: "Windows", "Linux", etc.
# ========================

# Creates a logging file for the current date time iff one does not exist.
def ensureLoggingFileIsCreated():
    logFilename = getCurrentLogfileName();
    if(os.path.exists(logFilename) == False):
        f = open(logFilename, "x");
        f.close();

# Writes a line to the logger with a timestamp.
def writeToLog(str):
    ensureLoggingFileIsCreated();
    logFile = open(getCurrentLogfileName(), 'a');
    strToWrite = getCurrentDateTimeStringForLog() + "\t---\t" + str + "\n";
    logFile.write(strToWrite);
    print(strToWrite);
    logFile.close();

# Returns the current UTC date string formatted appropriately for use in a filename.
def getCurrentDateStringForFilename():
    dt = datetime.now(tz.tzlocal());
    return dt.strftime("%Y_%m_%d-");

# Returns the current UTC datetime string formatted appropriately for use in a log file line.
def getCurrentDateTimeStringForLog():
    dt = datetime.now(tz.tzlocal());
    return dt.strftime("%Y-%m-%dT%H:%M:%S:%f%zZ");

# Returns the current UTC datetime string formatted appropriately for use in a filename.
def getCurrentDateTimeStringForFilename():
    dt = datetime.now(tz.tzlocal());
    return dt.strftime("%Y_%m_%dT%H_%M_%S_%f%z-");

# Returns a string for the current log filename formatted as 'Year_Month_Day-DTM_log.txt"
def getCurrentLogfileName():
    return getCurrentDateStringForFilename() + "DTM_log.txt";

# Returns an int with the 24-Hour 'Military Time' value ie (1300 = 1 PM). 
def get24HourMilitaryTimeInt():
    dt = datetime.now(tz.tzlocal());
    return (int(dt.strftime('%H')) * 100) + int(dt.strftime('%M'));

# Returns boolean 'True' if script is executed within the local hours when data should be uploaded, else returns False
def isInHoursOfOperation():
    currTime = get24HourMilitaryTimeInt();
    if(currTime >= startUploadTime and currTime <= endUploadTime):
        return True;
    return False;

# Compress a given file to LZMA format, see: https://www.cs.unb.ca/~bremner/teaching/cs2613/books/python3-doc/library/lzma.html
# Params: compression_level = standard is 6 but accepts 0-9, higher values are more compressed but are slower.
# Returns: filename string of compressed version of input file.
def ReplaceFileWithLZMAFile(filename, compression_level=6):
    returnedFilename = "";
    try:
        # filename will be saved as a '.xz' lzma compressed file
        newFilename = os.path.join(str(os.path.dirname(filename)), str(os.path.splitext(os.path.basename(filename))[0]) + ".xz");

        # Open original file as byte array, read bytes progressively and compress to .xz file.
        fh = open(filename, "rb");
        data = bytearray(mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ));
        obj = lzma.LZMACompressor(preset=compression_level);
        bindata = bytearray(b'');
        for i in data:
            b = i.to_bytes(1, sys.byteorder);
            cmpd = obj.compress(b);
            bindata += cmpd;
        bindata += obj.flush();
        fh.close();

        # Write binary data to file, overwriting if necessary.
        newFile = open(newFilename, "wb");
        newFile.write(bindata);
        newFile.close();

        # Delete old uncompressed file, return new filename to indicate success.        
        os.remove(filename);
        returnedFilename = newFilename;
    
    except Exception as e:
        writeToLog("ERROR! ReplaceFileWithLZMAFile() threw exception: " + str(e));
    
    return returnedFilename;

# Returns a random string of mixed uppper/lower case chars of N length.
def getRandomString(length):
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str;

# Creates a subfolder named with random chars to prevent collision with existing folder.
def createRandomSubfolder(startingDir, random_name_length):
    randomizedTestSubfolderName = getRandomString(random_name_length);
    newpath = os.path.join(startingDir, randomizedTestSubfolderName);
    if not os.path.exists(newpath):
        os.makedirs(newpath);
    return newpath;

# Removes an existing directory. NOTE: the directory must be empty.
def removeDirectory(dir):
    try:
        if(os.path.exists(dir)):
            os.rmdir(dir);
    except Exception as e:
        writeToLog('Failed to delete %s. Reason: %s' % (dir, str(e)));
        
# Removes any existing files from the given directory.
def removeAllFilesFromFolder(dir):
    if(os.path.exists(dir)):
        for filename in os.listdir(dir):
            file_path = os.path.join(dir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                writeToLog('Failed to delete %s. Reason: %s' + str(e));

# Removes all '\\' chars from a file path string that might cause errors.
def convertWindowsToUnixFilepath(windowsFilepath):
    windowsFilepath = windowsFilepath.replace("\\","/");
    return windowsFilepath.replace("//","/");

# Returns an int count with the number of files in a folder
def countFilesPerFolder(dir_path):
    count = 0;
    if(os.path.exists(dir_path)):
        for path in os.listdir(dir_path):
            if os.path.isfile(os.path.join(dir_path, path)):
                count += 1;
    return count;

# Return only the first file found using the os.listdir() function if there are any found, else return an empty string.
def getFirstFileInDirectory(dir_path):
    for path in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path, path)):
            return path;
    return "";

# Renames an existing file and prepends the datetime string to it.
def prependDateToFilename(old_filename):
    dirName = os.path.dirname(old_filename);
    currentDateTime = getCurrentDateTimeStringForFilename();
    baseName = os.path.basename(old_filename);
    new_filename = os.path.join(dirName, currentDateTime + baseName);
    os.rename(old_filename, new_filename);
    return new_filename;

# Uses safe copy functionality of specific OSes to transfer a single file. NOTE: Exception handling is tricky here since certain functions like robocopy 
# return a non-zero exit code on successful file transfer (ie, 1 = all files successfully transferred. 
# see: https://learn.microsoft.com/en-us/troubleshoot/windows-server/backup-and-storage/return-codes-used-robocopy-utility).
def platformSpecificMove(startPath, endPath):
        fp = open(getCurrentLogfileName(), "a");
        if(OStype == "Windows"): # Use robocopy on Windows
            subprocess.call(["robocopy", convertWindowsToUnixFilepath(os.path.dirname(startPath)), 
                                                 convertWindowsToUnixFilepath(os.path.dirname(endPath)), 
                                                 os.path.basename(endPath), "/MOVE"],stdout=fp,stderr=fp);
        elif(OStype == "Linux"): # Use rsync on Linux
             subprocess.call(["rsync", convertWindowsToUnixFilepath(startPath), convertWindowsToUnixFilepath(endPath), " -a"],stdout=fp,stderr=fp);
        else: # fallback to using the unsafe Python shell utils
            shutil.move(startPath,endPath);
            writeToLog("platformSpecificMove() successfully moved from " + convertWindowsToUnixFilepath(startPath) + 
                       " to " + convertWindowsToUnixFilepath(endPath) + " via shutil.move()");
        fp.close();

# Move every file in one folder to another using platform-specific robot file move functionality.
def platformSpecificFolderMove(startPath, endPath):
        fp = open(getCurrentLogfileName(), "a");
        if(OStype == "Windows"): # Use robocopy on Windows
            subprocess.call(["robocopy", startPath, endPath, "/MOV"],stdout=fp,stderr=fp);
        elif(OStype == "Linux"): # Use rsync on Linux
             subprocess.call(["rsync", convertWindowsToUnixFilepath(startPath), convertWindowsToUnixFilepath(endPath), " -a"],stdout=fp,stderr=fp);
        else: # fallback to using the unsafe Python shell utils
            shutil.move(startPath,endPath);
            writeToLog("platformSpecificMove() successfully moved from " + convertWindowsToUnixFilepath(startPath) + 
                       " to " + convertWindowsToUnixFilepath(endPath) + " via shutil.move()");
        fp.close();


# If there is more than one hdf5 file in the current directory, this function moves all .hdf5 files (other than the most recent one) from one directory to another, 
# ensuring their sha256 hashes match before/after moving them.
def moveDirectoryHDF5FilesLocalToNAS(directoryString, destinationFolder):
    writeToLog("Beginning process for moving files from local to NAS...");
    if(countFilesPerFolder(directoryString)==0 or (countFilesPerFolder(directoryString)==1 and getFirstFileInDirectory(directoryString).find(".hdf5") != -1)):
        writeToLog("No files to move! Ending early.");
        return;
    paths = sorted(Path(directoryString).iterdir(), key=os.path.getmtime); #get files in directory listed by last date modified from oldest to newest
    paths.pop(); # ignore the most recent file, which does not need to be archived.
    for file in paths:
         filename = os.fsdecode(file)
         if filename.endswith(".hdf5"):
             destinationFilepath = os.path.join(destinationFolder, os.path.basename(filename));
             writeToLog("Beginning archival process for file: " + filename);            
             filename = prependDateToFilename(filename);   # prepend the date of archival to the filename for easy lookup later.
             writeToLog("Changing origin file name to: " + str(filename) + " and compressing with LZMA...");
             filename = ReplaceFileWithLZMAFile(filename); # compress this file with lzma (as .xz file) and save new filename
             startingFilepath = filename;
             writeToLog("Compression finished. Moving file " + str(filename) + "...");
             destinationFilepath = os.path.join(destinationFolder, os.path.basename(filename)); # update destination with new .xz filename.
             platformSpecificMove(convertWindowsToUnixFilepath(startingFilepath), convertWindowsToUnixFilepath(destinationFilepath));
             writeToLog("Move finished. File is archived and stored at: " + convertWindowsToUnixFilepath(destinationFilepath));
         else:
             continue
         writeToLog("All files moved from local to NAS.");

# Move all files from given directory to another on the remote server.
def moveDirectoryHDF5FilesNAStoCloud(directoryString, destinationFolder):
    writeToLog("Beginning process for moving all files from NAS to remote...");
    platformSpecificFolderMove(convertWindowsToUnixFilepath(directoryString), convertWindowsToUnixFilepath(destinationFolder));
    writeToLog("All files moved from NAS to Remote.");
