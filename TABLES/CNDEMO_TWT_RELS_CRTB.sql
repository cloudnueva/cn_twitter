DROP TABLE cndemo_twtr_rels;
CREATE TABLE CNDEMO_TWTR_RELS
(tweet_id            NUMBER NOT NULL
,capture_id          NUMBER NOT NULL
,creation_date       TIMESTAMP WITH LOCAL TIME ZONE NOT NULL,
CONSTRAINT CNDEMO_TWTR_RELS_PK PRIMARY KEY (tweet_id,capture_id));

-- Trigger to handle record history fields.
CREATE OR REPLACE EDITIONABLE TRIGGER CNDEMO_TWTR_RELS_BI
BEFORE INSERT ON CNDEMO_TWTR_RELS FOR EACH ROW
BEGIN
  :new.creation_date := current_timestamp;
END;
/
INSERT INTO CNDEMO_TWTR_RELS (tweet_id,capture_id,creation_date)
SELECT DISTINCT tweet_id,capture_id,MIN(creation_date) FROM cndemo_twtr_tweets GROUP BY tweet_id,capture_id;