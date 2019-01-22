# Welcome to bigdataproj!

This a little project from my Master's degree at Tours University for Big Data, Cloud Computing et Services Web class. A project to practice and learn hbase manipulation, hadoop, mapreduce and REST

Members:
*Akachi, Ines
Nascimento Filho, Jessé Ferreira*

To execute this project on your own environment make sure that you have installed and setting:

 - Java SDK 1.8;
 - Hadoop version 2.8.4;
 - HBase version 1.2.6.1;
 - MapReduce;
 - Eclipse+m2e;

Our environment is a VM CentOS of Univ-Tours, thus the access to it is restricted to members of university. 


# How to:

**Load HBase:**  
$ `java -cp  'hbase classpath' :bigdataproj.jar:load_lib/* bdma.bigdata.aiwsbu.data.Setup`

load_lib/ is a directory with all dependecies of [hadoop](https://mvnrepository.com/artifact/org.apache.hadoop), [HBase](https://mvnrepository.com/search?q=HBase%20) and [javafaker](https://mvnrepository.com/artifact/com.github.javafaker/javafaker).


**Services start:**

After installation of hadoop and hbase;

`$ start-all.sh` 
`$ start-hbase.sh`
`$ hbase-daemon.sh start thrift`

Run Python WEB-SERVER:
<YOUR LOCAL DIR>/bigdata/webserve_hb`$ python webserve.py`

If some message like `Pipe is broken` appears, please re-running web-server, because it is a lean ws for test.


***Questions for test this project:***

Our REST URI execute query from follow templates such as below:

Question 1: \\ http://localhost:8080/aiwsbu/v1/students/[ID]/transcripts/[program] \\
e.i:\\
http://localhost:8080/aiwsbu/v1/students/2016000999/transcripts/L3

Question 2: \\ http://localhost:8080/aiwsbu/v1/rates/[semester]\\
e.i:\\
http://localhost:8080/aiwsbu/v1/rates/S07

Question 3 API: \\ http://localhost:8080/aiwsbu/v1/courses/[id]/rates\\
e.i:\\
http://localhost:8080/aiwsbu/v1/courses/S01A001/rates

Question 4: \\ http://localhost:8080/aiwsbu/v1/courses/[id]/rates/[year]\\
e.i:\\
http://localhost:8080/aiwsbu/v1/courses/S10B047/rates/2018

Question 5: \\ http://localhost:8080/aiwsbu/v1/programs/[program]/means/[year]\\
e.i:\\
http://localhost:8080/aiwsbu/v1/programs/M1/means/2007

Question 6: \\ http://localhost:8080/aiwsbu/v1/instructors/[name]/rates\\
e.i:\\
http://localhost:8080/aiwsbu/v1/instructors/Rhnoxk\%20Htrbdvhgw/rates

Question 7: \\ http://localhost:8080/aiwsbu/v1/ranks/[program]/years/[year]\\
e.i:\\
http://localhost:8080/aiwsbu/v1/ranks/M1/years/2007

References
-
https://hostpresto.com/community/tutorials/how-to-install-apache-hadoop-on-a-single-node-on-centos-7/
http://linuxpitstop.com/configure-distributed-hbase-cluster-on-centos-linux-7/
https://www.tutorialspoint.com/hbase/hbase_installation.htm
https://www.youtube.com/watch?v=bYaxXIRakxM
https://mvnrepository.com/
https://stackedit.io/
http://hbase.apache.org/0.94/book/mapreduce.example.html
https://gist.github.com/iaverin/f81720df9ed37a49ecee6341e4d5c0c6
https://stackoverflow.com/questions/21073041/url-route-matching-with-regex
http://txt2re.com/
https://docs.python.org/2/library/re.html
https://happybase.readthedocs.io/en/latest/index.html
