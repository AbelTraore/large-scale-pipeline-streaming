export SPARK_MAJOR_VERSION=2
spark-submit --class KPI_Streaming \
             --master yarn \
             --deploy-mode cluster \
             --num-executors ${MAVEN_EXECUTOR_NUMBER} \
             --executor-memory ${MAVEN_EXECUTOR_MEMORY} \
             --executor-cores ${MAVEN_EXECUTOR_CORES} \
             --driver-memory ${MAVEN_DRIVER_MEMORY } \
             --driver-core ${MAVEN_DRIVER_CORES} \
             --queue ${MAVEN_QUEUE_NAME} \
             ${MAVEN_JAR_PATH}


