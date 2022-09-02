DROP TABLE CNDEMO_TWTR_USERS;
CREATE TABLE CNDEMO_TWTR_USERS
(author_id           NUMBER NOT NULL
,username            VARCHAR2(100) NOT NULL
,name                VARCHAR2(100) NOT NULL
,location            VARCHAR2(100)
,profile_url         VARCHAR2(100) NOT NULL
,created_at          TIMESTAMP WITH LOCAL TIME ZONE
,followers_count     NUMBER NOT NULL
,following_count     NUMBER NOT NULL
,tweet_count         NUMBER NOT NULL
,profile_image_url   VARCHAR2(100)
,extra_url           VARCHAR2(100)
,creation_date       TIMESTAMP WITH LOCAL TIME ZONE NOT NULL
,last_update_date    TIMESTAMP WITH LOCAL TIME ZONE NOT NULL
,CONSTRAINT CNDEMO_TWTR_USERS_PK PRIMARY KEY (author_id));

CREATE INDEX CNDEMO_TWTR_USERS_U1 ON CNDEMO_TWTR_USERS (username);

-- Trigger to handle record history fields.
CREATE OR REPLACE EDITIONABLE TRIGGER CNDEMO_TWTR_USERS_BIU
BEFORE INSERT OR UPDATE ON CNDEMO_TWTR_USERS FOR EACH ROW
BEGIN
  IF INSERTING THEN
    :new.creation_date    := current_timestamp;
    :new.last_update_date := current_timestamp;
  ELSIF UPDATING THEN
    :new.last_update_date := current_timestamp;
  END IF;
END;