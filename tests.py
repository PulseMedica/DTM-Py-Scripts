import Filesystem;
import sys;

def moveFileTest(args):
    Filesystem.moveDirectoryFiles(Filesystem.convertWindowsToUnixFilepath(args[0]), Filesystem.convertWindowsToUnixFilepath(args[1]));
    return True;


# MAIN=========
if __name__ == '__main__':
    args = sys.argv[1:]
    if (len(args)<2):
        print("ERROR! Need input/output folder as args to move files");
        exit(-1);

    if(moveFileTest(args) == False):
        print("moveFileTest FAILED");
        exit(-2);
    else:
        print("moveFileTest PASSED");
    
    print("All Tests PASSED");
    exit(0);
