import DTM
import sys

# MAIN=========
if __name__ == '__main__':
    DTM.moveDirectoryHDF5FilesLocalToNAS(sys.argv[0],sys.argv[1]);
    print("'moveFilesLocalToNAS.py' script done executing.");
    print("Shutting down...");
    exit(0);
