import hashlib
import os
import shutil

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

# Moves all .db or .hdf5 files from one directory to another, ensuring their sha256 hashes match before/after moving them.
def moveDirectoryFiles(directoryString, destinationFolder):
    if(countFilesPerFolder(directoryString) == 0):
        print("No files to move! Ending early.");
        return;
    directory = os.fsencode(directoryString);
    filesToMove=[];
    print(directory);
    byteString = os.listdir(directory);
    for file in byteString:
         filename = os.fsdecode(file)
         if filename.endswith(".db") or filename.endswith(".hdf5"):
             startingFilepath = os.path.join(directory.decode("utf-8"), filename);
             destinationFilepath = os.path.join(destinationFolder, filename);
             originalHash = getFileHash(startingFilepath)
             shutil.move(startingFilepath,destinationFilepath);
             if(getFileHash(destinationFilepath)!=originalHash):
                print("ERROR! File checksums do not match");
             else:
                print("File successfully moved: " + destinationFilepath);
             continue
         else:
             continue
    return filesToMove;
