Scala IO talk
-------------

## USA data is in S3 
s3://scalaio_osm/usa.csv

## DL default spark dist :

*We use hadoop 2 !!! because of https://issues.apache.org/jira/browse/SPARK-2075*

```
curl http://d3kbcqa49mib13.cloudfront.net/spark-1.1.0-bin-hadoop2.4.tgz > spark.tgz
tar xvzf spark.tgz
rm spark.tgz
```

## Setup env for AWS
```
export AWS_ACCESS_KEY_ID=<YOUR-AWS-ACCESS-KEY>
export AWS_SECRET_ACCESS_KEY=<YOUR-AWS-SECRET-ACCESS-KEY>
```


## Head to spark ec2
```
cd spark*2.4
cd ec2

./spark-ec2   -k scalaio-osm \
              -i ~/.ssh/scalaio-osm.pem \
              --spark-version="1.1.0" \
              --region="eu-west-1" \
              --instance-type="m3.xlarge" \
              --no-ganglia \
              --hadoop-major-version="2" \
              -s 2 launch scalaio-osm
```

## Export master
```
export MASTER=$(./spark-ec2 -k scalaio-osm  -i ~/.ssh/scalaio-osm.pem  --region="eu-west-1" get-master scalaio-osm | grep ec2)

echo "********
********
$MASTER
********
********"
```

## Install S3cmd and put data in hdfs
```
ssh -o SendEnv -i ~/.ssh/scalaio-osm.pem root@$MASTER "

cd /mnt2

git clone https://github.com/s3tools/s3cmd.git
cd s3cmd
python setup.py install

cat <<KEY > ~/.s3cfg
[default]
access_key = $AWS_ACCESS_KEY_ID
access_token = 
add_encoding_exts = 
add_headers = 
bucket_location = US
cache_file = 
cloudfront_host = cloudfront.amazonaws.com
default_mime_type = binary/octet-stream
delay_updates = False
delete_after = False
delete_after_fetch = False
delete_removed = False
dry_run = False
enable_multipart = True
encoding = UTF-8
encrypt = False
expiry_date = 
expiry_days = 
expiry_prefix = 
follow_symlinks = False
force = False
get_continue = False
gpg_command = /usr/bin/gpg
gpg_decrypt = %(gpg_command)s -d --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
gpg_encrypt = %(gpg_command)s -c --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
gpg_passphrase = 
guess_mime_type = True
host_base = s3.amazonaws.com
host_bucket = %(bucket)s.s3.amazonaws.com
human_readable_sizes = False
ignore_failed_copy = False
invalidate_default_index_on_cf = False
invalidate_default_index_root_on_cf = True
invalidate_on_cf = False
list_md5 = False
log_target_prefix = 
max_delete = -1
mime_type = 
multipart_chunk_size_mb = 15
preserve_attrs = True
progress_meter = True
proxy_host = 
proxy_port = 0
put_continue = False
recursive = False
recv_chunk = 4096
reduced_redundancy = False
restore_days = 1
secret_key = $AWS_SECRET_ACCESS_KEY
send_chunk = 4096
server_side_encryption = False
simpledb_host = sdb.amazonaws.com
skip_existing = False
socket_timeout = 300
urlencoding_mode = normal
use_https = False
use_mime_magic = True
verbosity = WARNING
website_endpoint = http://%(bucket)s.s3-website-%(location)s.amazonaws.com/
website_error = 
website_index = index.html
KEY

s3cmd ls s3://scalaio_osm/

mkdir -p data
s3cmd get s3://scalaio_osm/usa.csv data/usa.csv
ls -la data
/root/ephemeral-hdfs/bin/hadoop fs -mkdir /data
/root/ephemeral-hdfs/bin/hadoop fs -put data/usa.csv /data
"
```

## Configure Hadoop Configuration for spark with AWS access keys
 **Sadly doens't seems to work => add aws keys in notebook :-/**

```
ssh -o SendEnv -i ~/.ssh/scalaio-osm.pem root@$MASTER "

# add keys to hadoop confiug of spark
sed -i \"s/<\/configuration>/  <property>\n <name>fs.s3n.awsAccessKeyId<\/name>\n <value>$AWS_ACCESS_KEY_ID<\/value>\n <\/property>\n <property>\n <name>fs.s3n.awsSecretAccessKey<\/name>\n <value>$AWS_SECRET_ACCESS_KEY<\/value>\n <\/property>\n <\/configuration>/\" /root/spark/conf/core-site.xml 

# copy new conf everywhere
/root/spark-ec2/copy-dir /root/spark/conf/

# stop spark
/root/spark/sbin/stop-all.sh 

# start spart
/root/spark/sbin/start-all.sh 

"
```

## Upload, setup project (incl scala-notebook)
```
ssh -i ~/.ssh/scalaio-osm.pem root@$MASTER <<'ENDSSH'
cd /mnt2

git clone https://github.com/randhindi/scalaio-talk.git

wget https://dl.bintray.com/sbt/native-packages/sbt/0.13.6/sbt-0.13.6.zip 
ls -la 
unzip sbt-0.13.6.zip
rm sbt-0.13.6.zip

ENDSSH
```

## Setup scala io project
```
ssh -i ~/.ssh/scalaio-osm.pem root@$MASTER <<'ENDSSH'
cd /mnt2

cd scalaio-talk/backend
/mnt2/sbt/bin/sbt publishLocal

# sync the jar in all spark nodes' lib folder
cp /root/.ivy2/local/scalaiotalk-mining/scalaiotalk-mining_2.10/0.1/jars/scalaiotalk-mining_2.10.jar /root/spark/lib
cd /root/spark-ec2/
./copy-dir /root/spark/lib/

ENDSSH
```

## Start scala-notebook 
```
ssh -i ~/.ssh/scalaio-osm.pem root@$MASTER <<'ENDSSH'
cd /mnt2
cd scalaio-talk/scala-notebook
. /root/spark/conf/spark-env.sh
export SPARK_EXECUTOR_MEMORY=12G
sh -c 'nohup /mnt2/sbt/bin/sbt "server/run --disable_security" > /var/log/notebook.log 2>&1 &'

ENDSSH
```

### Check the progress
#### log to the node
```
ssh  -L 8899:localhost:8899 -i ~/.ssh/scalaio-osm.pem root@$MASTER
```
#### tail the output
```
tail -f /var/log/notebook.log
```
## Open Notebook in browser
### in a term
```
ssh -i ~/.ssh/scalaio-osm.pem -L 8899:localhost:8899 root@$MASTER
```
### in the browser
```
http://localhost:8899
```

# Destroy
```
./spark-ec2 -k scalaio-osm  -i ~/.ssh/scalaio-osm.pem  --region="eu-west-1" destroy  scalaio-osm
```
