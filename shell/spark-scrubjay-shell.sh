DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function cmd-exists {
    command -v "$1" >/dev/null 2>&1
}

SCRUBJAR=$DIR/../target/scala-2.11/ScrubJay-assembly-1.0.jar
SCRUBINIT=$DIR/init_scrubjay.scala

if ! cmd-exists spark-shell; then
    echo "spark-shell command not found in PATH!"
    echo "Aborting"
    exit 1
fi

if [ -f $SCRUBJAR ]; then
    spark-shell \
        --name "ScrubJay Shell" \
        --master spark://sonar11:7077 \
        --driver-memory 128g \
        --executor-memory 127g \
        --conf spark.driver.maxresultsize=0 \
        --jars $SCRUBJAR \
        -i $SCRUBINIT
else
    echo "Assembly jar $SCRUBJAR not found! Run \`sbt assembly\` first!"
    echo "Aborting"
    exit 1
fi

