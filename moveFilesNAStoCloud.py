import DTM
import sys

# MAIN=========
if __name__ == '__main__':
    if(DTM.isInHoursOfOperation()):
        DTM.moveDirectoryHDF5FilesNAStoCloud(sys.argv[1],sys.argv[2]);
        print("'moveFilesNAStoCloud.py' script done executing.");
    else:
        print("'moveFilesNAStoCloud.py' script cannot run, not within upload hours of " + str(DTM.startUploadTime) + " - " + str(DTM.endUploadTime) + ".");
    print("Shutting down...");
    exit(0);
