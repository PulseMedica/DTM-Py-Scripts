import Filesystem;
import os;
import time;

# Test of 'Filesystem.moveDirectoryHDF5Files()' function. Will return boolean True if tests passes, else False.
def moveFileTest():

    # Create dummy files/folders.
    testFileRandomChars = 30;
    testStartDir = Filesystem.createRandomSubfolder(os.getcwd(), testFileRandomChars);
    testEndDir = Filesystem.createRandomSubfolder(os.getcwd(), testFileRandomChars);

    print(testStartDir);
    print(testEndDir);
    with open(os.path.join(testStartDir, "patient1.hdf5"), 'w') as fp:
        fp.write('test data1');
        fp.close();
    time.sleep(1.5); # pause between file creation so one file is noticably older than another.
    with open(os.path.join(testStartDir, "patient2.hdf5"), 'w') as fp:
        fp.write('test data2');
        fp.close();
    if(Filesystem.countFilesPerFolder(testStartDir) != 2):
        print('ERROR! Could not create necessary test files.');
        return False;

    # Run test.
    Filesystem.moveDirectoryHDF5Files(Filesystem.convertWindowsToUnixFilepath(testStartDir), Filesystem.convertWindowsToUnixFilepath(testEndDir));
    if(Filesystem.countFilesPerFolder(testStartDir) != 1 or Filesystem.countFilesPerFolder(testEndDir) != 1):
        print('ERROR! Could not successfully move test files.');
        return False;

    # Cleanup.
    Filesystem.removeAllFilesFromFolder(testStartDir);
    Filesystem.removeAllFilesFromFolder(testEndDir);

    if(Filesystem.countFilesPerFolder(testStartDir) != 0 or Filesystem.countFilesPerFolder(testEndDir) != 0):
        print('ERROR! Could not successfully remove test files.');
        return False;

    Filesystem.removeDirectory(testStartDir);
    Filesystem.removeDirectory(testEndDir);

    if(os.path.exists(testStartDir) or os.path.exists(testEndDir)):
        print("ERROR! Could not cleanup test directories properly");
        return False;

    return True;


# MAIN=========
if __name__ == '__main__':
    if(moveFileTest() == False):
        print("moveFileTest FAILED");
        exit(-2);
    else:
        print("moveFileTest PASSED");
    
    print("All Tests PASSED");
    exit(0);
