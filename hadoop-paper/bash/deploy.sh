set timeout 5
spawn scp ../target/hadoop-paper-1.0-SNAPSHOT.jar hdfs@datanode2.hadoop:~/biantao
expect "password:"
send "great@123*\n"
interact
