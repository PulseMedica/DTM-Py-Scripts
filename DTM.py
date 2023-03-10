## Data Transfer Manager
## written 3-10-2023
## 
## A collection of functions to backup and transfer
## files from local to NAS to cloud storage, to be
## imported and executed by scheduled tasks.
## ====================================================

from datetime import datetime
import hashlib
import lzma
import mmap
import os
from pathlib import Path
import random
import shutil
import string
import subprocess
import sys
import time

# CONSTANTS===============
startUploadTime = 1800; # Hours for uploading: 8 PM - 6 AM
endUploadTime = 600;
# ========================

# Returns an int with the 24-Hour 'Military Time' value ie (1300 = 1 PM). 
def get24HourMilitaryTimeInt():
    return (int(datetime.now().strftime('%H')) * 100) + int(datetime.now().strftime('%M'));

# Returns boolean 'True' if script is executed within the local hours when data should be uploaded, else returns False
def isInHoursOfOperation():
    currTime = get24HourMilitaryTimeInt();
    if(currTime >= startUploadTime and currTime <= endUploadTime):
        return True;
    return False;

# Compress a given file to LZMA format, see: https://www.cs.unb.ca/~bremner/teaching/cs2613/books/python3-doc/library/lzma.html
# Params: compression_level = standard is 6 but accepts 0-9, higher values are more compressed but are slower.
def ReplaceFileWithLZMAFile(filename, compression_level=6):
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
        return newFilename;
    
    except Exception as e:
        print("ERROR! ReplaceFileWithLZMAFile() threw exception: " + str(e));

    return "";

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
        os.rmdir(dir);
    except Exception as e:
        print('Failed to delete %s. Reason: %s' % (dir, e))
        
# Removes any existing files from the given directory.
def removeAllFilesFromFolder(dir):
    for filename in os.listdir(dir):
        file_path = os.path.join(dir, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

# Removes all '\\' chars from a file path string that might cause errors.
def convertWindowsToUnixFilepath(windowsFilepath):
    windowsFilepath = windowsFilepath.replace("\\","/");
    return windowsFilepath.replace("//","/");

# Get the sha256 hash for a file.
def getFileHash(fileToCheck):
    with open(fileToCheck, 'rb') as fileStream:
        data = fileStream.read();
        return hashlib.sha256(data).hexdigest();

# Returns an int count with the number of files in a folder
def countFilesPerFolder(dir_path):
    count = 0;
    for path in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path, path)):
            count += 1;
    return count;

def getFirstFileInDirectory(dir_path):
    for path in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path, path)):
            return path;
    return "";

# Renames an existing file and prepends the datetime string to it.
def prependDateToFilename(old_filename):
    dirName = os.path.dirname(old_filename);
    currentDateTime = datetime.utcnow().strftime("%m_%d_%YT%H_%M_%S_%fZ-");
    baseName = os.path.basename(old_filename);
    new_filename = os.path.join(dirName, currentDateTime + baseName);
    os.rename(old_filename, new_filename);
    return new_filename;

# If there is more than one hdf5 file in the current directory, this function moves all .hdf5 files (other than the most recent one) from one directory to another, 
# ensuring their sha256 hashes match before/after moving them.
def moveDirectoryHDF5FilesLocalToNAS(directoryString, destinationFolder):
    if(countFilesPerFolder(directoryString)==0 or (countFilesPerFolder(directoryString)==1 and getFirstFileInDirectory(directoryString).find(".hdf5") != -1)):
        print("No files to move! Ending early.");
        return;
    filesToMove=[];
    paths = sorted(Path(directoryString).iterdir(), key=os.path.getmtime); #get files in directory listed by last date modified from oldest to newest
    paths.pop(); # ignore the most recent file, which does not need to be archived.
    for file in paths:
         filename = os.fsdecode(file)
         if filename.endswith(".hdf5"):
             filename = prependDateToFilename(filename);   # prepend the date of archival to the filename for easy lookup later.
             filename = ReplaceFileWithLZMAFile(filename); # compress this file with lzma (as .xz file) and save new filename
             startingFilepath = filename;
             destinationFilepath = os.path.join(destinationFolder, os.path.basename(filename));
             originalHash = getFileHash(startingFilepath)
             shutil.move(startingFilepath,destinationFilepath);
             if(getFileHash(destinationFilepath)!=originalHash):
                print("ERROR! File checksums do not match");
             else:
                print("File successfully moved: " + destinationFilepath);
         else:
             continue
    return filesToMove;


# TO DO: finish
def moveDirectoryHDF5FilesNAStoCloud(directoryString, destinationFolder):
    return;
