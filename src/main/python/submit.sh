CONF_FILE=$1
PYSP_FILE=$2
DATA_FILE=$3
PARTITION=$4
#LOG_FILE=$5

spark-submit --properties-file $CONF_FILE $PYSP_FILE $DATA_FILE $PARTITION