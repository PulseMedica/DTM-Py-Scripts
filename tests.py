import DTM;
import os;
import time;

# Test of 'Filesystem.moveDirectoryHDF5FilesLocalToNAS()' function. Will return boolean True if tests passes, else False.
def MoveFilesToNASTest():

    # Create dummy files/folders.
    testFileRandomChars = 30;
    testStartDir = DTM.createRandomSubfolder(os.getcwd(), testFileRandomChars);
    testEndDir = DTM.createRandomSubfolder(os.getcwd(), testFileRandomChars);

    print(testStartDir);
    print(testEndDir);
    with open(os.path.join(testStartDir, "patient1.hdf5"), 'w') as fp:
        fp.write('test data1');
        fp.close();
    time.sleep(1.5); # pause between file creation so one file is noticably older than another.
    with open(os.path.join(testStartDir, "patient2.hdf5"), 'w') as fp:
        fp.write('test data2');
        fp.close();
    if(DTM.countFilesPerFolder(testStartDir) != 2):
        print('ERROR! Could not create necessary test files.');
        return False;

    # Run test.
    DTM.moveDirectoryHDF5FilesLocalToNAS(DTM.convertWindowsToUnixFilepath(testStartDir), DTM.convertWindowsToUnixFilepath(testEndDir));
    if(DTM.countFilesPerFolder(testStartDir) != 1 or DTM.countFilesPerFolder(testEndDir) != 1):
        print('ERROR! Could not successfully move test files.');
        return False;

    # Cleanup.
    DTM.removeAllFilesFromFolder(testStartDir);
    DTM.removeAllFilesFromFolder(testEndDir);

    if(DTM.countFilesPerFolder(testStartDir) != 0 or DTM.countFilesPerFolder(testEndDir) != 0):
        print('ERROR! Could not successfully remove test files.');
        return False;

    DTM.removeDirectory(testStartDir);
    DTM.removeDirectory(testEndDir);

    if(os.path.exists(testStartDir) or os.path.exists(testEndDir)):
        print("ERROR! Could not cleanup test directories properly");
        return False;

    return True;


# Test of check for whether or not script is executing within the hours designated for remote data upload.
def HoursOfOperationTest():
    currTime = DTM.get24HourMilitaryTimeInt();
    isInTime = (currTime >= DTM.startUploadTime and currTime <= DTM.endUploadTime);
    return (isInTime == DTM.isInHoursOfOperation());


# MAIN=========
if __name__ == '__main__':
    if(MoveFilesToNASTest() == False):
        print("MoveFilesToNASTest FAILED");
        exit(-2);
    else:
        print("MoveFilesToNASTest PASSED");
    
    if(HoursOfOperationTest() == False):
        print('HoursOfOperationTest FAILED');
        exit(-3);
    else:
        print("HoursOfOperationTest PASSED");

    print("All Tests PASSED");
    exit(0);
