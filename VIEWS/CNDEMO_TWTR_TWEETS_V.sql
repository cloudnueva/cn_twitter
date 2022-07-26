-- View Name: cndemo_twtr_tweets_v
CREATE OR REPLACE VIEW CNDEMO_TWTR_TWEETS_V AS
SELECT ctt.tweet_id
,      ctc.capture_id
,      ctt.author_id
,      ctu.profile_url
,      ctu.profile_url || '/status/' || ctt.tweet_id  tweet_url
,      ctc.name         capture_name
,      ctu.username     author_username
,      ctu.name         author_name
,      ctt.created_at
,      ctt.tweet_type_code
,      INITCAP(ctt.tweet_type_code) tweet_type_name
,      ctt.text
,      ctt.retweet_count
,      ctt.reply_count
,      ctt.quote_count
,      ctt.like_count
,      CASE WHEN tweet_type_code IN ('RETWEET','QUOTED','REPLY') THEN 1 ELSE 0 END non_original_count
,      CASE tweet_type_code WHEN 'ORIGINAL' THEN 1 ELSE 0 END                      original_count
,      (retweet_count + quote_count + reply_count) non_original_sum
,      CASE 
         WHEN tweet_type_code = 'ORIGINAL' THEN 5 + ((retweet_count + quote_count + reply_count)*.5) + (.25 * like_count)
         WHEN tweet_type_code IN ('RETWEET','QUOTED','REPLY') THEN 5
         ELSE 0
       END  tweet_score 
FROM   cndemo_twtr_tweets  ctt
,      cndemo_twtr_users   ctu
,      cndemo_twtr_capture ctc
,      cndemo_twtr_rels    ctr
WHERE  ctr.capture_id = ctc.capture_id
AND    ctr.tweet_id   = ctt.tweet_id
AND    ctt.author_id  = ctu.author_id;
