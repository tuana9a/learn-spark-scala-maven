spark-submit \
--class com.tuana9a.learn.spark.Main \
--master yarn \
--deploy-mode cluster \
--driver-memory 1G \
--executor-memory 1G \
--executor-cores 4 \
--files config.properties \
learn-spark-scala-maven-0.0.1.jar \
TestAny 0