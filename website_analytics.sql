-- Create User table
CREATE TABLE users (
  id STRING PRIMARY KEY
) WITH (
  kafka_topic='users', 
  value_format='AVRO'
);

-- Create Pageviews stream
CREATE STREAM pageviews WITH (
  kafka_topic='pageviews',
  value_format='AVRO' 
);

-- Rekey Pageviews stream to use userid
CREATE STREAM pageviews_rekeyed WITH (
  kafka_topic='pageviews-rekeyed',
  value_format='AVRO' 
) AS
SELECT 
  * 
FROM PAGEVIEWS 
PARTITION BY USERID
EMIT CHANGES;

-- Filter out information from Pageviews
CREATE STREAM pageviews_filtered WITH (
  kafka_topic='pageviews-filtered',
  value_format='AVRO' 
) AS
SELECT 
  ROWTIME AS pageviewtime,
  * 
FROM PAGEVIEWS_REKEYED 
WHERE USERID != 'User_1' 
EMIT CHANGES;

-- Enrich Pageviews with User data
CREATE STREAM pageviews_enriched WITH (
  kafka_topic='pageviews-enriched',
  value_format='AVRO'
) AS 
SELECT 
  pageviews.viewtime                    AS viewtime,
  FROM_UNIXTIME(pageviews.pageviewtime) AS pageviewtime,
  pageviews.pageid                      AS pageid,
  pageviews.userid                      AS userid,
  users.regionid                        AS regionid,
  users.gender                          AS gender,
  FROM_UNIXTIME(users.registertime)     AS registertime
FROM pageviews_filtered AS pageviews
INNER JOIN users
ON pageviews.userid = users.id
EMIT CHANGES;

-- Conduct regional analysis
CREATE TABLE pageview_regional_analysis WITH (   
  kafka_topic='pageview-regional-analysis',
  format='AVRO'
) AS 
SELECT
  regionid,
  COUNT(*) AS views,
  AVG(viewtime) AS viewtme_avg 
FROM pageviews_enriched
WINDOW TUMBLING (SIZE 30 MINUTES)
GROUP BY regionid
EMIT FINAL;