~/dev/spark/current/bin/spark-submit \
    --driver-library-path /opt/cloudera/parcels/CDH/lib/hadoop/lib/native   \
    --master yarn \
    --deploy-mode client   \
    --num-executors 128 \
    --executor-memory 2g    \
    --driver-memory 6g     \
    --conf spark.driver.maxResultSize=8192m \
    --class edu.umd.cs.hcil.analytics.spark.topics.twitter.TwitterTopicModeler \
    /cliphomes/cbuntain/Development/CrossPlatformAnalytics/target/CrossPlatformAnalytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
    -i '/collections/tweets/TweetsCrawl/us-west/2016-06/statuses.log.*.gz' \
    -o topic_models/twitter_201606_$TOPIC_COUNT \
    -s ~/stops.txt \
    -t $TOPIC_COUNT \
    -i 128 \
    --mintf 1 \
    --mindf 1000 \
    -p 2048


~/dev/spark/current/bin/spark-submit \
    --driver-library-path /opt/cloudera/parcels/CDH/lib/hadoop/lib/native   \
    --master yarn \
    --deploy-mode client   \
    --num-executors 128 \
    --executor-memory 2g    \
    --driver-memory 6g     \
    --conf spark.driver.maxResultSize=8192m \
    --class edu.umd.cs.hcil.analytics.spark.topics.twitter.TwitterTopicModeler \
    /cliphomes/cbuntain/Development/CrossPlatformAnalytics/target/CrossPlatformAnalytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
    -i '/collections/tweets/TweetsCrawl/us-west/2016-07/statuses.log.*.gz' \
    -o topic_models/twitter_201607_$TOPIC_COUNT \
    -s ~/stops.txt \
    -t $TOPIC_COUNT \
    -i 128 \
    --mintf 1 \
    --mindf 1000 \
    -p 2048

echo ~/dev/spark/current/bin/spark-submit \
    --driver-library-path /opt/cloudera/parcels/CDH/lib/hadoop/lib/native   \
    --master yarn \
    --deploy-mode client   \
    --num-executors 128 \
    --executor-memory 2g    \
    --driver-memory 6g     \
    --conf spark.driver.maxResultSize=8192m \
    --class edu.umd.cs.hcil.analytics.spark.topics.twitter.TwitterTopicModeler \
    /cliphomes/cbuntain/Development/CrossPlatformAnalytics/target/CrossPlatformAnalytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
    -i '/collections/tweets/TweetsCrawl/us-west/2016-08/statuses.log.*.gz' \
    -o topic_models/twitter_201608_$TOPIC_COUNT \
    -s ~/stops.txt \
    -t $TOPIC_COUNT \
    -i 128 \
    --mintf 1 \
    --mindf 1000 \
    -p 2048


~/dev/spark/current/bin/spark-submit \
    --driver-library-path /opt/cloudera/parcels/CDH/lib/hadoop/lib/native   \
    --master yarn \
    --deploy-mode client   \
    --num-executors 128 \
    --executor-memory 2g    \
    --driver-memory 6g     \
    --conf spark.driver.maxResultSize=8192m \
    --class edu.umd.cs.hcil.analytics.spark.topics.twitter.TwitterTopicModeler \
    /cliphomes/cbuntain/Development/CrossPlatformAnalytics/target/CrossPlatformAnalytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
    -i '/collections/tweets/TweetsCrawl/us-west/2016-09/statuses.log.*.gz' \
    -o topic_models/twitter_201609_$TOPIC_COUNT \
    -s ~/stops.txt \
    -t $TOPIC_COUNT \
    -i 128 \
    --mintf 1 \
    --mindf 1000 \
    -p 2048


~/dev/spark/current/bin/spark-submit \
    --driver-library-path /opt/cloudera/parcels/CDH/lib/hadoop/lib/native   \
    --master yarn \
    --deploy-mode client   \
    --num-executors 128 \
    --executor-memory 2g    \
    --driver-memory 6g     \
    --conf spark.driver.maxResultSize=8192m \
    --class edu.umd.cs.hcil.analytics.spark.topics.twitter.TwitterTopicModeler \
    /cliphomes/cbuntain/Development/CrossPlatformAnalytics/target/CrossPlatformAnalytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
    -i '/collections/tweets/TweetsCrawl/us-west/2016-10/statuses.log.*.gz' \
    -o topic_models/twitter_201610_$TOPIC_COUNT \
    -s ~/stops.txt \
    -t $TOPIC_COUNT \
    -i 128 \
    --mintf 1 \
    --mindf 1000 \
    -p 2048


~/dev/spark/current/bin/spark-submit \
    --driver-library-path /opt/cloudera/parcels/CDH/lib/hadoop/lib/native   \
    --master yarn \
    --deploy-mode client   \
    --num-executors 128 \
    --executor-memory 2g    \
    --driver-memory 6g     \
    --conf spark.driver.maxResultSize=8192m \
    --class edu.umd.cs.hcil.analytics.spark.topics.twitter.TwitterTopicModeler \
    /cliphomes/cbuntain/Development/CrossPlatformAnalytics/target/CrossPlatformAnalytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
    -i '/collections/tweets/TweetsCrawl/us-west/2016-11/statuses.log.*.gz' \
    -o topic_models/twitter_201611_$TOPIC_COUNT \
    -s ~/stops.txt \
    -t $TOPIC_COUNT \
    -i 128 \
    --mintf 1 \
    --mindf 1000 \
    -p 2048


~/dev/spark/current/bin/spark-submit \
    --driver-library-path /opt/cloudera/parcels/CDH/lib/hadoop/lib/native   \
    --master yarn \
    --deploy-mode client   \
    --num-executors 128 \
    --executor-memory 2g    \
    --driver-memory 6g     \
    --conf spark.driver.maxResultSize=8192m \
    --class edu.umd.cs.hcil.analytics.spark.topics.twitter.TwitterTopicModeler \
    /cliphomes/cbuntain/Development/CrossPlatformAnalytics/target/CrossPlatformAnalytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
    -i '/collections/tweets/TweetsCrawl/us-west/2016-12/statuses.log.*.gz' \
    -o topic_models/twitter_201612_$TOPIC_COUNT \
    -s ~/stops.txt \
    -t $TOPIC_COUNT \
    -i 128 \
    --mintf 1 \
    --mindf 1000 \
    -p 2048
