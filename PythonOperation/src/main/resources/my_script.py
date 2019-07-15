import sys

# Print out the input parameter
input1 = sys.argv[1]
print('Hello world', input1)

import os
cwd = os.getcwd()
print("Current working directory is: ", cwd)
print("files in the root directory of the container are: ", next(os.walk('.')))
print("files in the hostBindMount directory of the container are: ", next(os.walk('/hostBindMount')))

os.chdir("/hostBindMount")

# Open a file for writing and create it if it doesn't exist
f = open("testFile" + input1 + ".txt","w+")

# Write some lines of data to the file
for i in range(10):
    f.write("This is a line \n")

# Close the file
f.close()
print("Created a new file in the hostBindMount directory")

print("files in the root directory of the container are: ", next(os.walk('.')))
print("files in the hostBindMount directory of the container are: ", next(os.walk('/hostBindMount')))