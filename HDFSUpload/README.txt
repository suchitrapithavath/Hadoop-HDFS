*******************************************************CS226-Assignment-1******************************************************************8

HDFSUpload folder contains the pom.xml,src and target folder where target got created by running maven package.

Following command is used to run the file:

hadoop jar HDFSUpload-1.0-SNAPSHOT.jar edu.ucr.cs.cs226.spith001.HDFSUpload AREAWATER.csv AREAWATERhdfsfile.csv



Java program that copies a file from the local machine to HDFS and performs three tasks to measure the performance of the file system and compare the performance of the LocalFileSystem to the DistributedFileSystem.

The total time for copying the 2GB file provided in the instructions below.
The total time for reading a 2GB sequentially from the start to the end.
The total time to make 2,000 random accesses, each of size 1KB. To test this, generate a random position in the file, seek to that position, and read 1 KB.

