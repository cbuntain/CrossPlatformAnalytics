#!/bin/bash

NUM_EXEC=128
PARTS=1024

for DATE in 07 08 09 10 11 12
do 

    PREDATE=`expr $DATE - 1`
    PREDATE=`printf "%02d" $PREDATE`
    echo $PREDATE, $DATE

    for TCOUNT in 100 # 10 50 200
    do
        echo "    " $TCOUNT

        T2R_LOG="t2r_2016${PREDATE}_2016${DATE}_T${TCOUNT}.log"
        T2T_LOG="t2t_2016${PREDATE}_2016${DATE}_T${TCOUNT}.log"
        R2R_LOG="r2r_2016${PREDATE}_2016${DATE}_T${TCOUNT}.log"
        R2T_LOG="r2t_2016${PREDATE}_2016${DATE}_T${TCOUNT}.log"

        # Evaluate Reddit models on itself and Twitter
        ~/dev/spark/current/bin/spark-submit \
         --driver-library-path /opt/cloudera/parcels/CDH/lib/hadoop/lib/native   \
         --master yarn \
         --deploy-mode client   \
         --num-executors $NUM_EXEC \
         --executor-memory 6g    \
         --driver-memory 6g     \
         --class edu.umd.cs.hcil.analytics.spark.topics.EvaluateModel \
         /cliphomes/cbuntain/Development/CrossPlatformAnalytics/target/CrossPlatformAnalytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
         -d "/user/cbuntain/reddit/RS_2016-$DATE.bz2" \
         -m /user/cbuntain/topic_models/reddit_2016${PREDATE}_${TCOUNT}_lda.model \
         -v /user/cbuntain/topic_models/reddit_2016${PREDATE}_${TCOUNT}_vectorizer.model \
         -s ~/stops.txt \
         -n 1000000 \
         -p $PARTS > $R2R_LOG


        ~/dev/spark/current/bin/spark-submit \
         --driver-library-path /opt/cloudera/parcels/CDH/lib/hadoop/lib/native   \
         --master yarn \
         --deploy-mode client   \
         --num-executors $NUM_EXEC \
         --executor-memory 6g    \
         --driver-memory 6g     \
         --class edu.umd.cs.hcil.analytics.spark.topics.EvaluateModel \
         /cliphomes/cbuntain/Development/CrossPlatformAnalytics/target/CrossPlatformAnalytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
         -d "/collections/tweets/TweetsCrawl/us-west/2016-${DATE}/statuses.log.*.gz" \
         -m /user/cbuntain/topic_models/reddit_2016${PREDATE}_${TCOUNT}_lda.model \
         -v /user/cbuntain/topic_models/reddit_2016${PREDATE}_${TCOUNT}_vectorizer.model \
         -s ~/stops.txt \
         -n 1000000 \
         -p $PARTS > $R2T_LOG

         # Evaluate Twitter models on itself and Reddit
         ~/dev/spark/current/bin/spark-submit \
         --driver-library-path /opt/cloudera/parcels/CDH/lib/hadoop/lib/native   \
         --master yarn \
         --deploy-mode client   \
         --num-executors $NUM_EXEC \
         --executor-memory 6g    \
         --driver-memory 6g     \
         --class edu.umd.cs.hcil.analytics.spark.topics.EvaluateModel \
         /cliphomes/cbuntain/Development/CrossPlatformAnalytics/target/CrossPlatformAnalytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
         -d "/user/cbuntain/reddit/RS_2016-$DATE.bz2" \
         -m /user/cbuntain/topic_models/twitter_2016${PREDATE}_${TCOUNT}_lda.model \
         -v /user/cbuntain/topic_models/twitter_2016${PREDATE}_${TCOUNT}_vectorizer.model \
         -s ~/stops.txt \
         -n 1000000 \
         -p $PARTS > $T2R_LOG


        ~/dev/spark/current/bin/spark-submit \
         --driver-library-path /opt/cloudera/parcels/CDH/lib/hadoop/lib/native   \
         --master yarn \
         --deploy-mode client   \
         --num-executors $NUM_EXEC \
         --executor-memory 6g    \
         --driver-memory 6g     \
         --class edu.umd.cs.hcil.analytics.spark.topics.EvaluateModel \
         /cliphomes/cbuntain/Development/CrossPlatformAnalytics/target/CrossPlatformAnalytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
         -d "/collections/tweets/TweetsCrawl/us-west/2016-${DATE}/statuses.log.*.gz" \
         -m /user/cbuntain/topic_models/twitter_2016${PREDATE}_${TCOUNT}_lda.model \
         -v /user/cbuntain/topic_models/twitter_2016${PREDATE}_${TCOUNT}_vectorizer.model \
         -s ~/stops.txt \
         -n 1000000 \
         -p $PARTS > $T2T_LOG

    done

done
