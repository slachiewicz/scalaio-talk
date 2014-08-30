
#--master spark://207.184.161.138:7077 \
#--total-executor-cores 100 \

/root/spark/bin/spark-submit --class scalaiotalk.Main --executor-memory 6G /root/scalaiotalk-mining.jar