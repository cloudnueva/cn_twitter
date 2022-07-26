-- View Name: cndemo_twtr_tweets_stats_v
CREATE OR REPLACE VIEW CNDEMO_TWTR_TWEETS_STATS_V AS
SELECT ctr.capture_id
,      ctt.tweet_id
,      ctt.author_id
,      ctt.created_at
,      ctt.tweet_type_code
,      ctt.retweet_count
,      ctt.reply_count
,      ctt.like_count
,      ctt.quote_count
,      (retweet_count + quote_count + reply_count) non_original_sum
FROM   cndemo_twtr_tweets  ctt
,      cndemo_twtr_rels    ctr
WHERE  ctr.tweet_id = ctt.tweet_id;
