import hashlib
import os
from pathlib import Path
import random
import shutil
import string

# Returns a random string of mixed uppper/lower case chars of N length.
def get_random_string(length):
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str;

# Creates a subfolder named with random chars to prevent collision with existing folder.
def createRandomSubfolder(startingDir, random_name_length):
    randomizedTestSubfolderName = get_random_string(random_name_length);
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

# If there is more than one hdf5 file in the current directory, this function moves all .hdf5 files (other than the most recent one) from one directory to another, 
# ensuring their sha256 hashes match before/after moving them.
def moveDirectoryHDF5Files(directoryString, destinationFolder):
    if(countFilesPerFolder(directoryString)==0 or (countFilesPerFolder(directoryString)==1 and getFirstFileInDirectory(directoryString).find(".hdf5") != -1)):
        print("No files to move! Ending early.");
        return;
    filesToMove=[];
    print(directoryString);
    paths = sorted(Path(directoryString).iterdir(), key=os.path.getmtime); #get files in directory listed by last date modified from oldest to newest
    print(paths);
    paths.pop(); #remove the oldest recent file
    print(paths);
    for file in paths:
         filename = os.fsdecode(file)
         if filename.endswith(".hdf5"):
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
