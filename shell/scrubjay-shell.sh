# Get location of this file
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Sanitize
SCRUBJAR="$DIR/../target/scala-2.11/ScrubJay-assembly-1.0.jar"

if [ ! -f $SCRUBJAR ]; then
    echo "Assembly jar $SCRUBJAR not found! Run \`sbt assembly\` first!"
    echo "Aborting"
    exit 1
fi

if ! command -v spark-shell >/dev/null 2>&1; then
    echo "spark-shell command not found in PATH!"
    echo "Aborting"
    exit 1
fi

# Set defaults
SPARK_MASTER="local[*]"
CASSANDRA_HOST="localhost"
CASSANDRA_USER="cassandra"
CASSANDRA_PASSWORD="cassandra"
INPUT_SCRIPT="/dev/null"

# Parse arguments
while [[ $# -gt 1 ]]
do
    key="$1"
    case $key in
        -m|--master)
        SPARK_MASTER="$2"
        shift
        ;;
        -c|--cassandra-host)
        CASSANDRA_HOST="$2"
        shift
        ;;
        -u|--cassandra-user)
        CASSANDRA_USER="$2"
        shift
        ;;
        -p|--cassandra-password)
        CASSANDRA_PASSWORD="$2"
        shift
        ;;
        -i|--input-script)
        INPUT_SCRIPT="$2"
        shift
        ;;
        *)
        echo "Unknown argument!"
        ;;
    esac
    shift
done

# Spark me up
spark-shell \
    --name "ScrubJay Shell" \
    --master $SPARK_MASTER \
    --conf spark.max.cores=32 \
    --conf spark.executor.cores=32 \
    --conf spark.default.parallelism=128 \
    --conf spark.driver.maxResultSize=0 \
    --conf spark.cassandra.connection.host=$CASSANDRA_HOST \
    --conf spark.cassandra.auth.username=$CASSANDRA_USER \
    --conf spark.cassandra.auth.password=$CASSANDRA_PASSWORD \
    --conf spark.cassandra.output.ignoreNulls=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.registrator=scrubjay.registrator.KryoRegistrator \
    --jars $SCRUBJAR \
    -i $INPUT_SCRIPT
