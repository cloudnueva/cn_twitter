CREATE OR REPLACE PACKAGE CNDEMO_TWTR_UTL_PK AS
-----------------------------------------------------------------------------------
-- NAME        : cndemo_twtr_utl_pk
-- FILE NAME   : CNDEMO_TWTR_UTL_PKB.sql
-- REVISION    : $2022.1.0$
-- PURPOSE     : Package containing code Twitter Capture App.
--
-- DELIVERED BY: $jdixon$
--               
-- Revision History:
-- VER        DATE         AUTHOR           DESCRIPTION
-- ========   ===========  ================ =======================================
-- 2022.1.0   18-JUL-2022  jdixon           Created.
-----------------------------------------------------------------------------------

  -----------------------
  -- Global Constants  --
  -----------------------
  GC_TWTR_RECENT_API_URL        CONSTANT VARCHAR2(500) := 'https://api.twitter.com/2/tweets/search/recent';
  GC_TWTR_TWEET_API_URL         CONSTANT VARCHAR2(500) := 'https://api.twitter.com/2/tweets';
  GC_TWTR_USER_API_URL          CONSTANT VARCHAR2(500) := 'https://api.twitter.com/2/users/by';
  GC_TWTR_TOKEN_API_URL         CONSTANT VARCHAR2(500) := 'https://api.twitter.com/oauth2/token';
  GC_TWTR_RECENT_CREDENTIAL_ID  CONSTANT VARCHAR2(500) := 'TWITTER_CREDENTIALS';
  GC_TWTR_API_TIMEOUT_SECS      CONSTANT NUMBER        := 10;
  GC_TWTR_RECENT_MAX_RESULTS    CONSTANT NUMBER        := 100;
  GC_TWTR_RECENT_MAX_ITERATIONS CONSTANT NUMBER        := 10;
  GC_TWEET_TYPE_ORIGINAL        CONSTANT cndemo_twtr_tweets.tweet_type_code%TYPE := 'ORIGINAL';
  GC_TWEET_TYPE_RETWEET         CONSTANT cndemo_twtr_tweets.tweet_type_code%TYPE := 'RETWEET';
  GC_TWEET_TYPE_QUOTED          CONSTANT cndemo_twtr_tweets.tweet_type_code%TYPE := 'QUOTED';
  GC_TWEET_TYPE_REPLY           CONSTANT cndemo_twtr_tweets.tweet_type_code%TYPE := 'REPLY';
  
  GC_USER_FIELDS                CONSTANT VARCHAR2(500) := 'username,name,public_metrics,profile_image_url,created_at,location,url';
  GC_TWEET_FIELDS               CONSTANT VARCHAR2(500) := 'created_at,public_metrics,referenced_tweets';
  
-- Procedure called by APEX Automation
PROCEDURE capture_tweets;

-- Refresh Older Tweets
PROCEDURE refresh_tweets;

-- Refresh User Details
PROCEDURE refresh_users;

-- Run before the load of each page.
PROCEDURE on_load
 (p_page_id    IN NUMBER,
  p_capture_id IN cndemo_twtr_capture.capture_id%TYPE);

PROCEDURE page30_load
 (p_capture_id IN cndemo_twtr_capture.capture_id%TYPE);
 
END CNDEMO_TWTR_UTL_PK;
/
SHOW ERR;