DROP TABLE CNDEMO_TWTR_TWEETS;
CREATE TABLE CNDEMO_TWTR_TWEETS
(tweet_id            NUMBER NOT NULL
,tweet_type_code     VARCHAR2(10) NOT NULL
,author_id           NUMBER NOT NULL
,created_at          TIMESTAMP WITH LOCAL TIME ZONE
,text                VARCHAR2(32000) NOT NULL
,retweet_count       NUMBER NOT NULL
,reply_count         NUMBER NOT NULL
,like_count          NUMBER NOT NULL
,quote_count         NUMBER NOT NULL
,creation_date       TIMESTAMP WITH LOCAL TIME ZONE NOT NULL
,last_update_date    TIMESTAMP WITH LOCAL TIME ZONE NOT NULL
,CONSTRAINT CNDEMO_TWTR_RELS_PK PRIMARY KEY (tweet_id));

-- Create Indexes.
CREATE INDEX CNDEMO_TWTR_TWEETS_N1 ON CNDEMO_TWTR_TWEETS (created_at, author_id);
CREATE INDEX CNDEMO_TWTR_TWEETS_N2 ON CNDEMO_TWTR_TWEETS (tweet_type_code, created_at);

-- Trigger to handle record history fields.
CREATE OR REPLACE EDITIONABLE TRIGGER CNDEMO_TWTR_TWEETS_BIU
BEFORE INSERT OR UPDATE ON CNDEMO_TWTR_TWEETS FOR EACH ROW
BEGIN
  IF INSERTING THEN
    :new.creation_date    := current_timestamp;
    :new.last_update_date := current_timestamp;
  ELSIF UPDATING THEN
    :new.last_update_date := current_timestamp;
  END IF;
END;