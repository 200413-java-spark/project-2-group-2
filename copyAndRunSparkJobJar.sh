# $1 is where your pem file is located (keypairfile) ex. ~/.ssh/linux-demo.pem
# $2 where jar is located

if [ "$1" != "Usage" ]; then
	scp -i $1 $2 hadoop@3.15.7.69:~
	ssh -i $1 hadoop@3.15.7.69 spark-submit ./spark-job-1.0-SNAPSHOT.jar
else
	echo "Usage"
	echo "./copyAndRunSparkJobJar.sh [pem file location] [jar location]"
fi
