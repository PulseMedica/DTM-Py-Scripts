import hashlib
import os
import shutil
import sys

def moveFile(oldFilepath, newFilepath):
    shutil.move(oldFilepath, newFilepath);

def getFileHash(fileToCheck):
    with open(fileToCheck, 'rb') as fileStream:
        data = fileStream.read();
        return hashlib.md5(data).hexdigest();

def moveAllMongoDBOrHDF5FilesInDirectory(directoryString, destinationFolder):
    directory = os.fsencode(directoryString);
    filesToMove=[];
    for file in os.listdir(directory):
         filename = os.fsdecode(file)
         if filename.endswith(".db") or filename.endswith(".hdf5"):
             startingFilepath = os.path.join(directory.decode("utf-8"), filename);
             destinationFilepath = os.path.join(destinationFolder, filename);
             originalHash = getFileHash(startingFilepath)
             moveFile(startingFilepath,destinationFilepath);
             if(getFileHash(destinationFilepath)!=originalHash):
                print("ERROR! File checksums do not match");
             else:
                print("File successfully moved: " + destinationFilepath);
             continue
         else:
             continue
    return filesToMove;

if __name__ == '__main__':
    args = sys.argv[1:]
    if (len(args)<2):
        print("ERROR! Need input/output folder as args to move files");
    else:
        moveAllMongoDBOrHDF5FilesInDirectory(args[0],args[1]);
