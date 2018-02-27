# Files Deduplication and Replication:

This project involves development of files deduplication and replication in the file system using Big Data Analytics

### Software Requirements:
- Java
- Apache Hadoop(MapReduce Framework

### Instructions to execute:

0. Create a folder with some images or txt.files
1. Compress the files(not folder) to be analyzed in tar format
2. In the terminal go to the location where tar-to-seq.jar is present
3. Run the following command : **java -jar tar-to-seq.jar "tarfilename".tar.gz "sequencefilename".seq**
4. Open the source code in eclipse(Both the class files-Deduplication.java and Replication.java)
5. Add the paths as specified in the comments
6. Create jar by selecting both the files.
7. Import the sequence file created in step 2 to hdfs
8. Run line for Deduplication: **hadoop jar "jarname".jar Deduplication "Sequence File Location" "output path"**
9. Run line for Replication:   **hadoop jar "jarname".jar Replication "Sequence File Location" "output path"**
