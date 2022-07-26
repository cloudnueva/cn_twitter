SET DEFINE OFF;
CREATE OR REPLACE PACKAGE BODY CNDEMO_TWTR_UTL_PK AS
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

  GC_SCOPE_PREFIX  CONSTANT VARCHAR2(100) := LOWER($$plsql_unit) || '.';

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
PROCEDURE refresh_summaries (p_type IN VARCHAR2) IS
  CURSOR cr_capture_summary IS
     SELECT ctr.capture_id
     ,      MIN(created_at)  oldest_tweet
     ,      COUNT(1)         total_tweets
     ,      SUM(like_count)  total_likes
     ,      COUNT(DISTINCT author_id) total_authors
     FROM   cndemo_twtr_tweets ctt
     ,      cndemo_twtr_rels   ctr
     WHERE  ctt.tweet_id   = ctr.tweet_id
     GROUP BY ctr.capture_id;
BEGIN
  -- Summarize Information by Capture.
  FOR r_capture_summary IN cr_capture_summary LOOP
     -- Set the Last Load Time for the Capture.
     UPDATE cndemo_twtr_capture
     SET    last_capture  = CASE p_type WHEN 'HISTORY' THEN last_capture ELSE current_timestamp END
     ,      oldest_tweet  = r_capture_summary.oldest_tweet
     ,      total_tweets  = r_capture_summary.total_tweets
     ,      total_likes   = r_capture_summary.total_likes
     ,      total_authors = r_capture_summary.total_authors
     WHERE  capture_id    = r_capture_summary.capture_id;
  END LOOP;
END refresh_summaries;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
PROCEDURE handle_user 
  (p_user_rec IN OUT NOCOPY cndemo_twtr_users%ROWTYPE,
   p_action   IN VARCHAR2) IS
  l_logger_scope  logger_logs.SCOPE%TYPE := GC_SCOPE_PREFIX || utl_call_stack.subprogram(1)(2);
  l_logger_params logger.tab_param;
  l_user_exists   PLS_INTEGER;
BEGIN

  p_user_rec.profile_url := 'https://twitter.com/'||p_user_rec.username;

  IF p_action = 'CU' THEN
    SELECT COUNT(1) INTO l_user_exists
    FROM   cndemo_twtr_users
    WHERE  author_id = p_user_rec.author_id;
  ELSE
    l_user_exists := 1;
  END IF;

  IF l_user_exists = 1 THEN
    UPDATE cndemo_twtr_users
    SET    name              = p_user_rec.name
    ,      location          = p_user_rec.location
    ,      created_at        = p_user_rec.created_at
    ,      extra_url         = p_user_rec.extra_url
    ,      profile_url       = p_user_rec.profile_url
    ,      followers_count   = p_user_rec.followers_count
    ,      following_count   = p_user_rec.following_count
    ,      tweet_count       = p_user_rec.tweet_count
    ,      profile_image_url = p_user_rec.profile_image_url
    WHERE  author_id         = p_user_rec.author_id;
  ELSE
    INSERT INTO cndemo_twtr_users VALUES p_user_rec;
  END IF;

EXCEPTION WHEN OTHERS THEN
  logger.log_error('Unhandled Error ['||SQLERRM||']', l_logger_scope, NULL, l_logger_params);
  RAISE;  
END handle_user;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
PROCEDURE handle_users
 (p_users  IN JSON_ARRAY_T,
  p_action IN VARCHAR2) IS

  l_logger_scope        logger_logs.SCOPE%TYPE := GC_SCOPE_PREFIX || utl_call_stack.subprogram(1)(2);
  l_logger_params       logger.tab_param;
  l_user_count          PLS_INTEGER;
  l_date_time_str       VARCHAR2(50);
  l_user_obj            JSON_OBJECT_T;
  lr_user_rec           cndemo_twtr_users%ROWTYPE;
  lr_user_rec_miss      cndemo_twtr_users%ROWTYPE;

BEGIN
  l_user_count := p_users.get_size;
  IF l_user_count > 0 THEN
    FOR i IN 0..l_user_count -1 LOOP
      lr_user_rec                   := lr_user_rec_miss;
      l_user_obj                    := JSON_OBJECT_T(p_users.get(i));
      lr_user_rec.author_id         := l_user_obj.get_Number('id');
      lr_user_rec.username          := l_user_obj.get_String('username');
      lr_user_rec.name              := l_user_obj.get_String('name');
      lr_user_rec.location          := l_user_obj.get_String('location');
      lr_user_rec.extra_url         := l_user_obj.get_String('url');
      l_date_time_str               := l_user_obj.get_String('created_at');
      SELECT TO_UTC_TIMESTAMP_TZ(l_date_time_str) INTO lr_user_rec.created_at FROM sys.dual;
      lr_user_rec.profile_image_url := l_user_obj.get_String('profile_image_url');
      lr_user_rec.followers_count   := l_user_obj.get_Object('public_metrics').get_Number('followers_count');
      lr_user_rec.following_count   := l_user_obj.get_Object('public_metrics').get_Number('following_count');
      lr_user_rec.tweet_count       := l_user_obj.get_Object('public_metrics').get_Number('tweet_count');
      -- Create / Update User.
      handle_user (p_user_rec => lr_user_rec, p_action => p_action);
    END LOOP;
  ELSE
    logger.log_warn('  No User Records', l_logger_scope, NULL, l_logger_params);
  END IF;
END handle_users;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
PROCEDURE handle_tweet 
  (p_capture_id IN cndemo_twtr_capture.capture_id%TYPE,
   p_tweet_rec  IN cndemo_twtr_tweets%ROWTYPE,
   p_action     IN VARCHAR2) IS
  l_logger_scope  logger_logs.SCOPE%TYPE := GC_SCOPE_PREFIX || utl_call_stack.subprogram(1)(2);
  l_logger_params logger.tab_param;
  l_tweet_exists  PLS_INTEGER;
  l_rel_exists    PLS_INTEGER;
BEGIN

  IF p_action = 'CU' THEN
    -- If we are in Create/Update Mode then Check if the Tweet Exists.
    SELECT COUNT(1) INTO l_tweet_exists
    FROM   cndemo_twtr_tweets
    WHERE  tweet_id = p_tweet_rec.tweet_id;
  ELSE
    l_tweet_exists := 1;
  END IF;
  
  IF l_tweet_exists = 0 THEN

    -- Create the Tweet and Relationship.
    INSERT INTO cndemo_twtr_tweets VALUES p_tweet_rec;
    INSERT INTO cndemo_twtr_rels (tweet_id,capture_id) VALUES (p_tweet_rec.tweet_id, p_capture_id);

  ELSE
    
    -- Check if we need to create a relationship record.
    IF p_action = 'CU' THEN
      SELECT COUNT(1) INTO l_rel_exists
      FROM   cndemo_twtr_rels
      WHERE  tweet_id   = p_tweet_rec.tweet_id
      AND    capture_id = p_capture_id;
      IF l_rel_exists = 0 THEN
        INSERT INTO cndemo_twtr_rels (tweet_id,capture_id) VALUES (p_tweet_rec.tweet_id, p_capture_id);
      END IF;
    END IF;
    
    -- Update Tweet Details.
    UPDATE cndemo_twtr_tweets
    SET    retweet_count = p_tweet_rec.retweet_count
    ,      reply_count   = p_tweet_rec.reply_count
    ,      like_count    = p_tweet_rec.like_count
    ,      quote_count   = p_tweet_rec.quote_count
    WHERE  tweet_id      = p_tweet_rec.tweet_id;
  END IF;

EXCEPTION WHEN OTHERS THEN
  logger.log_error('Unhandled Error ['||SQLERRM||']', l_logger_scope, NULL, l_logger_params);
  RAISE;
END handle_tweet;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
FUNCTION get_tweet_type (p_twtr_refs IN JSON_ARRAY_T) 
  RETURN cndemo_twtr_tweets.tweet_type_code%TYPE IS
  l_twtr_refs_count     PLS_INTEGER;
  l_tweet_type_code     cndemo_twtr_tweets.tweet_type_code%TYPE;
  l_is_retweet          BOOLEAN := FALSE;
  l_is_quoted           BOOLEAN := FALSE;
  l_is_reply            BOOLEAN := FALSE;

BEGIN
  l_twtr_refs_count := p_twtr_refs.get_size;
  FOR i IN 0..l_twtr_refs_count -1 LOOP
    CASE JSON_OBJECT_T(p_twtr_refs.get(i)).get_String('type')
      WHEN 'retweeted'  THEN l_is_retweet := TRUE;
      WHEN 'quoted'     THEN l_is_quoted  := TRUE;
      WHEN 'replied_to' THEN l_is_reply   := TRUE;
      ELSE NULL;
    END CASE;
  END LOOP;
  RETURN( 
    CASE 
      WHEN l_is_retweet THEN GC_TWEET_TYPE_RETWEET
      WHEN l_is_quoted  THEN GC_TWEET_TYPE_QUOTED
      WHEN l_is_reply   THEN GC_TWEET_TYPE_REPLY
      ELSE GC_TWEET_TYPE_ORIGINAL
    END);

END get_tweet_type;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
PROCEDURE handle_tweets
  (p_tweets     IN JSON_ARRAY_T,
   p_capture_id IN cndemo_twtr_capture.capture_id%TYPE,
   p_action     IN VARCHAR2) IS

  l_tweet_count         PLS_INTEGER;
  l_tweet_obj           JSON_OBJECT_T;
  lr_tweet_rec          cndemo_twtr_tweets%ROWTYPE;
  lr_tweet_rec_miss     cndemo_twtr_tweets%ROWTYPE;
  l_date_time_str       VARCHAR2(50);

BEGIN

  l_tweet_count := p_tweets.get_size;

  FOR i IN 0..l_tweet_count -1 LOOP
    lr_tweet_rec               := lr_tweet_rec_miss;
    l_tweet_obj                := JSON_OBJECT_T(p_tweets.get(i));
    lr_tweet_rec.tweet_id      := l_tweet_obj.get_Number('id');
    lr_tweet_rec.author_id     := l_tweet_obj.get_Number('author_id');
    l_date_time_str            := l_tweet_obj.get_String('created_at');
    SELECT TO_UTC_TIMESTAMP_TZ(l_date_time_str) INTO lr_tweet_rec.created_at FROM sys.dual;
    lr_tweet_rec.text          := l_tweet_obj.get_String('text');
    lr_tweet_rec.retweet_count := l_tweet_obj.get_Object('public_metrics').get_Number('retweet_count');
    lr_tweet_rec.reply_count   := l_tweet_obj.get_Object('public_metrics').get_Number('reply_count');
    lr_tweet_rec.like_count    := l_tweet_obj.get_Object('public_metrics').get_Number('like_count');
    lr_tweet_rec.quote_count   := l_tweet_obj.get_Object('public_metrics').get_Number('quote_count');
    
    -- Determine the Tweet Type.
    IF l_tweet_obj.has ('referenced_tweets') THEN
      lr_tweet_rec.tweet_type_code := get_tweet_type (p_twtr_refs => l_tweet_obj.get_Array('referenced_tweets'));
    ELSE 
      lr_tweet_rec.tweet_type_code := GC_TWEET_TYPE_ORIGINAL;
    END IF;
  
    -- Create/Update the Tweet
    handle_tweet (p_capture_id => p_capture_id, p_tweet_rec => lr_tweet_rec, p_action => p_action);
    
  END LOOP;

END handle_tweets;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
PROCEDURE capture_tweets IS

  CURSOR cr_captures IS
    SELECT capture_id
    ,      name
    ,      query_value
    FROM   cndemo_twtr_capture;

  l_logger_scope        logger_logs.SCOPE%TYPE := GC_SCOPE_PREFIX || utl_call_stack.subprogram(1)(2);
  l_logger_params       logger.tab_param;
  http_request_failed   exception;
  pragma exception_init (http_request_failed, -29273); 
  lt_parm_names         apex_application_global.VC_ARR2;
  lt_parm_values        apex_application_global.VC_ARR2;
  TYPE capture_t        IS TABLE OF cr_captures %ROWTYPE INDEX BY BINARY_INTEGER;
  lt_captures           capture_t;
  l_twtr_json           CLOB;
  l_next_token          VARCHAR2(50);
  l_twtr_object         JSON_OBJECT_T;
  l_result_count        PLS_INTEGER;
  l_batch_number        PLS_INTEGER;

BEGIN

  logger.log('START', l_logger_scope, NULL, l_logger_params);
  
  -- Fetch List of Capture Identifiers
  OPEN  cr_captures;
  FETCH cr_captures BULK COLLECT INTO lt_captures;
  CLOSE cr_captures;

  lt_parm_names.DELETE();
  lt_parm_values.DELETE();
  
  -- Default Web Service Parameters
  lt_parm_names(1)  := 'query';
  lt_parm_values(1) := NULL;
  lt_parm_names(2)  := 'tweet.fields';
  lt_parm_values(2) := GC_TWEET_FIELDS;
  lt_parm_names(3)  := 'expansions';
  lt_parm_values(3) := 'author_id';
  lt_parm_names(4)  := 'user.fields';
  lt_parm_values(4) := GC_USER_FIELDS;
  lt_parm_names(5)  := 'max_results';
  lt_parm_values(5) := GC_TWTR_RECENT_MAX_RESULTS;

  FOR i IN 1..lt_captures.COUNT() LOOP
    -- Initialize Values for the Capture.
    l_batch_number    := 0;
    lt_parm_names(6)  := NULL;
    lt_parm_values(6) := NULL;
    logger.log('Start Capture ['||lt_captures(i).name||']', l_logger_scope, NULL, l_logger_params);

    -- Set Query Parameter for the capture.
    lt_parm_values(1) := lt_captures(i).query_value;
    
    LOOP
      l_logger_params.DELETE();
      l_batch_number := l_batch_number + 1;
      logger.log(' > Start Batch ['||l_batch_number||']', l_logger_scope, NULL, l_logger_params);

      BEGIN
        -- Get Payload.
        l_twtr_json := apex_web_service.make_rest_request
         (p_url                  => GC_TWTR_RECENT_API_URL,
          p_http_method          => 'GET',
          p_transfer_timeout     => GC_TWTR_API_TIMEOUT_SECS,
          p_parm_name            => lt_parm_names,
          p_parm_value           => lt_parm_values,
          p_credential_static_id => GC_TWTR_RECENT_CREDENTIAL_ID,
          p_token_url            => GC_TWTR_TOKEN_API_URL);
      EXCEPTION WHEN http_request_failed THEN
        logger.append_param(l_logger_params, 'twtr_json', l_twtr_json);
        logger.log_error('Twitter API Timed out', l_logger_scope, NULL, l_logger_params);
        RETURN;
      END;
      
      -- Check if the call was sucessful.
      IF apex_web_service.g_status_code != 200 THEN
        logger.append_param(l_logger_params, 'response_code', apex_web_service.g_status_code);
        logger.append_param(l_logger_params, 'twtr_json', l_twtr_json);
        logger.log_error('Twitter API  Failed', l_logger_scope, NULL, l_logger_params);
        RETURN;
      END IF;
  
      -- Parse the Response.
      l_twtr_object := JSON_OBJECT_T.PARSE(l_twtr_json);

      -- Get the token for the next set of records (if there is one).
      l_next_token   := l_twtr_object.get_Object('meta').get_String('next_token');
      l_result_count := l_twtr_object.get_Object('meta').get_String('result_count');
      logger.append_param(l_logger_params, 'next_token', l_next_token);
      logger.append_param(l_logger_params, 'result_count', l_result_count);
  
      IF l_result_count > 0 THEN
      
        -- Process Tweets in 'data' array.
        handle_tweets 
          (p_tweets     => l_twtr_object.get_Array('data'),
           p_capture_id => lt_captures(i).capture_id,
           p_action     => 'CU');
        
        -- Process the Twitter 'users' array.
        handle_users (p_users => l_twtr_object.get_Object('includes').get_Array('users'), p_action => 'CU');
        
        -- Decide if we need to fetch more records.
        IF l_next_token IS NULL OR l_batch_number > GC_TWTR_RECENT_MAX_ITERATIONS THEN
          -- Exit Loop of Web Service Calls.
          EXIT;
        ELSE 
          lt_parm_names(6)  := 'next_token';
          lt_parm_values(6) := l_next_token;
        END IF;

       ELSE
         -- No Tweets found in Response.
         logger.log_warn('No Results from Twitter API '||l_twtr_json, l_logger_scope, NULL, l_logger_params);
         EXIT;
       END IF;

      logger.log(' > End Batch ['||l_batch_number||']', l_logger_scope, NULL, l_logger_params);

     END LOOP;

     COMMIT;

    logger.log('End Capture ['||lt_captures(i).name||']', l_logger_scope, NULL, l_logger_params);

  END LOOP;

  -- Refresh Summary Capture Levels Stats.
  refresh_summaries (p_type => 'CURRENT');
  
  COMMIT;

  logger.log('END', l_logger_scope, NULL, l_logger_params);

EXCEPTION WHEN OTHERS THEN
  logger.log_error('Unhandled Error ['||SQLERRM||']', l_logger_scope, NULL, l_logger_params);
END capture_tweets;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
PROCEDURE refresh_tweets IS

  CURSOR cr_tweets IS
    SELECT tweet_id
    FROM   cndemo_twtr_tweets
    WHERE  created_at BETWEEN (CURRENT_TIMESTAMP - INTERVAL '21' DAY) AND  (CURRENT_TIMESTAMP - INTERVAL '6' DAY);

  l_logger_scope        logger_logs.SCOPE%TYPE := GC_SCOPE_PREFIX || utl_call_stack.subprogram(1)(2);
  l_logger_params       logger.tab_param;
  http_request_failed   exception;
  pragma exception_init (http_request_failed, -29273); 
  lt_parm_names         apex_application_global.VC_ARR2;
  lt_parm_values        apex_application_global.VC_ARR2;
  TYPE tweets_t         IS TABLE OF cr_tweets%ROWTYPE INDEX BY BINARY_INTEGER;
  lt_tweets             tweets_t;
  l_twtr_json           CLOB;
  l_twtr_object         JSON_OBJECT_T;
  l_tweet_count         PLS_INTEGER;
  l_total_count         PLS_INTEGER := 0;
  l_iteration_count     PLS_INTEGER := 0;
  l_batch_number        PLS_INTEGER := 0;
  l_tweet_list          VARCHAR2(32000);

BEGIN

  -- Get Count of Tweets that are between 7 and 21 days old.
  OPEN  cr_tweets;
  FETCH cr_tweets BULK COLLECT INTO lt_tweets;
  CLOSE cr_tweets;
  l_tweet_count := lt_tweets.COUNT();
  logger.append_param(l_logger_params, 'tweet_count', l_tweet_count);
  logger.log('START', l_logger_scope, NULL, l_logger_params);
  
  IF l_tweet_count = 0 THEN
    logger.log_warn('Nothing to do.', l_logger_scope, NULL, l_logger_params);
    RETURN;
  END IF;

  -- Set Default Web Service Parameters
  lt_parm_names.DELETE();
  lt_parm_values.DELETE();
  lt_parm_names(1)  := 'ids';
  lt_parm_values(1) := NULL;
  lt_parm_names(2)  := 'tweet.fields';
  lt_parm_values(2) := GC_TWEET_FIELDS;
  lt_parm_names(3)  := 'expansions';
  lt_parm_values(3) := 'author_id';
  lt_parm_names(4)  := 'user.fields';
  lt_parm_values(4) := GC_USER_FIELDS;

  -- Loop through Tweets  
  FOR i IN 1..l_tweet_count LOOP
    l_total_count     := l_total_count + 1;
    l_iteration_count := l_iteration_count + 1;
    
    IF (l_iteration_count >= 100 OR l_total_count >= l_tweet_count) THEN
      l_logger_params.DELETE();
      l_batch_number := l_batch_number + 1;
      logger.append_param(l_logger_params, 'iteration_count', l_iteration_count);
      logger.log(' > Start Batch ['||l_batch_number||']', l_logger_scope, NULL, l_logger_params);
      lt_parm_values(1) := l_tweet_list;
      BEGIN
        -- Get Payload.
        l_twtr_json := apex_web_service.make_rest_request
         (p_url                  => GC_TWTR_TWEET_API_URL,
          p_http_method          => 'GET',
          p_transfer_timeout     => GC_TWTR_API_TIMEOUT_SECS,
          p_parm_name            => lt_parm_names,
          p_parm_value           => lt_parm_values,
          p_credential_static_id => GC_TWTR_RECENT_CREDENTIAL_ID,
          p_token_url            => GC_TWTR_TOKEN_API_URL);
      EXCEPTION WHEN http_request_failed THEN
        logger.append_param(l_logger_params, 'twtr_json', l_twtr_json);
        logger.log_error('Twitter API Timed out', l_logger_scope, NULL, l_logger_params);
        RETURN;
      END;

      -- Check if the call was sucessful.
      IF apex_web_service.g_status_code != 200 THEN
        logger.append_param(l_logger_params, 'response_code', apex_web_service.g_status_code);
        logger.append_param(l_logger_params, 'twtr_json', l_twtr_json);
        logger.log_error('Twitter API  Failed', l_logger_scope, NULL, l_logger_params);
        RETURN;
      END IF;

      -- Parse the Response.
      l_twtr_object := JSON_OBJECT_T.PARSE(l_twtr_json);

      -- Process Tweets in 'data' array.
      handle_tweets 
        (p_tweets     => l_twtr_object.get_Array('data'),
         p_capture_id => NULL,
         p_action     => 'U');

      -- Process the Twitter 'users' array.
      handle_users (p_users => l_twtr_object.get_Object('includes').get_Array('users'), p_action => 'CU');

      l_iteration_count := 0;
      l_tweet_list      := NULL;
      logger.log(' > End Batch ['||l_batch_number||']', l_logger_scope, NULL, l_logger_params);

    ELSE

      -- Add Tweet to List.
      IF l_iteration_count = 1 THEN
        l_tweet_list := l_tweet_list || lt_tweets(i).tweet_id;
      ELSE
        l_tweet_list := l_tweet_list || ','||lt_tweets(i).tweet_id;
      END IF;
    END IF;

  END LOOP;

  -- Refresh Summary Capture Levels Stats.
  refresh_summaries (p_type => 'CURRENT');
  
  COMMIT;

  logger.log('END', l_logger_scope, NULL, l_logger_params);

EXCEPTION WHEN OTHERS THEN
  logger.log_error('Unhandled Error ['||SQLERRM||']', l_logger_scope, NULL, l_logger_params);
END refresh_tweets;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
PROCEDURE refresh_users IS

  CURSOR cr_users IS
    SELECT username
    FROM   cndemo_twtr_users;

  l_logger_scope        logger_logs.SCOPE%TYPE := GC_SCOPE_PREFIX || utl_call_stack.subprogram(1)(2);
  l_logger_params       logger.tab_param;
  http_request_failed   exception;
  pragma exception_init (http_request_failed, -29273); 
  lt_parm_names         apex_application_global.VC_ARR2;
  lt_parm_values        apex_application_global.VC_ARR2;
  TYPE users_t          IS TABLE OF cr_users %ROWTYPE INDEX BY BINARY_INTEGER;
  lt_users              users_t;
  l_user_json           CLOB;
  l_user_object         JSON_OBJECT_T;
  l_user_count          PLS_INTEGER;
  l_total_count         PLS_INTEGER := 0;
  l_iteration_count     PLS_INTEGER := 0;
  l_username_list       VARCHAR2(32000);
  l_batch_number        PLS_INTEGER := 0;

BEGIN

  -- Fetch all Twitter Users into a PL/SQL table.
  OPEN  cr_users;
  FETCH cr_users BULK COLLECT INTO lt_users;
  CLOSE cr_users;
  l_user_count := lt_users.COUNT();
  logger.append_param(l_logger_params, 'user_count', l_user_count);
  logger.log('START', l_logger_scope, NULL, l_logger_params);
  
  IF l_user_count = 0 THEN
    logger.log_warn('Nothing to do.', l_logger_scope, NULL, l_logger_params);
    RETURN;
  END IF;

  -- Set Default Web Service Parameters
  lt_parm_names.DELETE();
  lt_parm_values.DELETE();
  lt_parm_names(1)  := 'usernames';
  lt_parm_values(1) := NULL;
  lt_parm_names(2)  := 'user.fields';
  lt_parm_values(2) := GC_USER_FIELDS;

  -- Loop through Users  
  FOR i IN 1..l_user_count LOOP
    l_total_count     := l_total_count + 1;
    l_iteration_count := l_iteration_count + 1;
    
    IF (l_iteration_count >= 100 OR l_total_count >= l_user_count) THEN
      l_logger_params.DELETE();
      l_batch_number := l_batch_number + 1;
      logger.append_param(l_logger_params, 'iteration_count', l_iteration_count);
      logger.log(' > Start Batch ['||l_batch_number||']', l_logger_scope, NULL, l_logger_params);
      lt_parm_values(1) := l_username_list;
      BEGIN
        -- Get Payload.
        l_user_json := apex_web_service.make_rest_request
         (p_url                  => GC_TWTR_USER_API_URL,
          p_http_method          => 'GET',
          p_transfer_timeout     => GC_TWTR_API_TIMEOUT_SECS,
          p_parm_name            => lt_parm_names,
          p_parm_value           => lt_parm_values,
          p_credential_static_id => GC_TWTR_RECENT_CREDENTIAL_ID,
          p_token_url            => GC_TWTR_TOKEN_API_URL);
      EXCEPTION WHEN http_request_failed THEN
        logger.append_param(l_logger_params, 'user_json', l_user_json);
        logger.log_error('Twitter User API Timed out', l_logger_scope, NULL, l_logger_params);
        RETURN;
      END;

      -- Check if the call was sucessful.
      IF apex_web_service.g_status_code != 200 THEN
        logger.append_param(l_logger_params, 'response_code', apex_web_service.g_status_code);
        logger.append_param(l_logger_params, 'user_json', l_user_json);
        logger.log_error('Twitter User API Failed', l_logger_scope, NULL, l_logger_params);
        RETURN;
      END IF;

      -- Parse the Response.
      l_user_object := JSON_OBJECT_T.PARSE(l_user_json);

      -- Process the user 'data' array.
      handle_users (p_users => l_user_object.get_Array('data'), p_action => 'U');
       
      l_iteration_count := 0;
      l_username_list   := NULL;
      logger.log(' > End Batch ['||l_batch_number||']', l_logger_scope, NULL, l_logger_params);

    ELSE

      -- Add Username to List.
      IF l_iteration_count = 1 THEN
        l_username_list := l_username_list || lt_users(i).username;
      ELSE
        l_username_list := l_username_list || ','||lt_users(i).username;
      END IF;
    END IF;

  END LOOP;

  COMMIT;

  logger.log('END', l_logger_scope, NULL, l_logger_params);

EXCEPTION WHEN OTHERS THEN
  logger.log_error('Unhandled Error ['||SQLERRM||']', l_logger_scope, NULL, l_logger_params);
END refresh_users;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
PROCEDURE on_load
 (p_page_id    IN NUMBER,
  p_capture_id IN cndemo_twtr_capture.capture_id%TYPE) IS
  l_logger_scope        logger_logs.SCOPE%TYPE := GC_SCOPE_PREFIX || utl_call_stack.subprogram(1)(2);
  l_logger_params       logger.tab_param;
  l_redirect_url        VARCHAR2(500);
BEGIN
  IF p_page_id NOT IN (1,2) AND p_capture_id IS NULL THEN
    -- Goto Home Page.
    l_redirect_url := apex_page.get_url (p_page => 'home');
    BEGIN
      apex_util.redirect_url (p_url => l_redirect_url);
    EXCEPTION WHEN apex_application.e_stop_apex_engine THEN
      NULL;
    END;
  END IF;
EXCEPTION WHEN OTHERS THEN
  logger.log_error('Unhandled Error ['||SQLERRM||']', l_logger_scope, NULL, l_logger_params);
END on_load;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
PROCEDURE page30_load
 (p_capture_id IN cndemo_twtr_capture.capture_id%TYPE) IS

  CURSOR cr_capture_info IS
    SELECT name, image_url
    FROM   cndemo_twtr_capture
    WHERE  capture_id = p_capture_id;

  l_logger_scope        logger_logs.SCOPE%TYPE := GC_SCOPE_PREFIX || utl_call_stack.subprogram(1)(2);
  l_logger_params       logger.tab_param;
  lr_capture_info       cr_capture_info%ROWTYPE;
  l_dow_sql             CONSTANT VARCHAR2(1000) := q'[
WITH totals AS
(SELECT TRUNC(created_at) created_at
,       COUNT(1)          tweet_count
,       SUM(like_count)   total_likes
FROM   cndemo_twtr_tweets_stats_v
WHERE  tweet_type_code = 'ORIGINAL'
AND    capture_id      = :CAPTURE_ID
GROUP BY TRUNC(created_at))
SELECT TO_CHAR(created_at, 'D')   day_number
,      CASE 
         WHEN COUNT(DISTINCT created_at) = 0 THEN 0 ELSE
          ROUND(SUM(tweet_count) / COUNT(DISTINCT created_at),0)
       END avg_tweets_day
,      CASE 
         WHEN COUNT(DISTINCT created_at) = 0 THEN 0 ELSE
          ROUND(SUM(total_likes) / COUNT(DISTINCT created_at),0)
       END avg_likes_day
,      0
,      0
,      NULL
,      NULL
,      NULL
,      NULL
,      NULL
,      TO_CHAR(created_at, 'Day') day_label
FROM   totals
GROUP BY TO_CHAR(created_at, 'Day'), TO_CHAR(created_at, 'D')]';

  l_by_week_sql     CONSTANT VARCHAR2(1000) := q'[
SELECT COUNT(1)                tweet_total
,      SUM(non_original_sum)   retweet_total
,      SUM(like_count)         like_total
,      0
,      0
,      TRUNC(created_at,'WW')  posted_date
,      NULL
,      NULL
,      NULL
,      NULL
FROM   cndemo_twtr_tweets_stats_v
WHERE  capture_id = :CAPTURE_ID
GROUP BY TRUNC(created_at,'WW')]';

BEGIN

  -- Populate Page Variables
  OPEN  cr_capture_info;
  FETCH cr_capture_info INTO lr_capture_info;
  CLOSE cr_capture_info;  
  apex_util.set_session_state('P30_CAPTURE_NAME',lr_capture_info.name);
  apex_util.set_session_state('P30_CAPTURE_URL',lr_capture_info.image_url);
  
  -- Create Collection for BY Day of Week Chart
  apex_collection.create_collection_from_queryb2
   (p_collection_name    => 'CHART_BY_DOW',
    p_query              => l_dow_sql,
    p_names              => apex_util.string_to_table('CAPTURE_ID'),
    p_values             => apex_util.string_to_table(p_capture_id),
    p_truncate_if_exists => 'YES');

  -- Create Collection for By Week
  apex_collection.create_collection_from_queryb2
   (p_collection_name    => 'CHART_BY_WEEK',
    p_query              => l_by_week_sql,
    p_names              => apex_util.string_to_table('CAPTURE_ID'),
    p_values             => apex_util.string_to_table(p_capture_id),
    p_truncate_if_exists => 'YES');

EXCEPTION WHEN OTHERS THEN
  logger.log_error('Unhandled Error ['||SQLERRM||']', l_logger_scope, NULL, l_logger_params);
END page30_load;


END CNDEMO_TWTR_UTL_PK;
/
SHOW ERR;