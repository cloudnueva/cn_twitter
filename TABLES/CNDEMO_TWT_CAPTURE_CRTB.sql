-- cndemo_twtr_capture
DROP TABLE CNDEMO_TWTR_CAPTURE;
CREATE TABLE CNDEMO_TWTR_CAPTURE
(capture_id          NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY CONSTRAINT CNDEMO_TWTR_CAPTURE_PK PRIMARY KEY NOT NULL
,query_value         VARCHAR2(1000) NOT NULL
,name                VARCHAR2(50)   NOT NULL
,description         VARCHAR2(1000)
,image_url           VARCHAR2(500)
,last_capture        TIMESTAMP WITH LOCAL TIME ZONE
,oldest_tweet        TIMESTAMP WITH LOCAL TIME ZONE
,total_tweets        NUMBER
,total_authors       NUMBER
,total_likes         NUMBER
,creation_date       TIMESTAMP WITH LOCAL TIME ZONE NOT NULL
,created_by          VARCHAR2(255) NOT NULL
,last_update_date    TIMESTAMP WITH LOCAL TIME ZONE NOT NULL
,last_updated_by     VARCHAR2(255) NOT NULL);

CREATE UNIQUE INDEX CNDEMO_TWTR_CAPTURE_U1 ON CNDEMO_TWTR_CAPTURE (name);

-- Trigger to handle record history fields.
CREATE OR REPLACE EDITIONABLE TRIGGER CNDEMO_TWTR_CAPTURE_BIU
BEFORE INSERT OR UPDATE ON CNDEMO_TWTR_CAPTURE FOR EACH ROW
BEGIN
  IF INSERTING THEN
    :new.created_by       := COALESCE(SYS_CONTEXT('APEX$SESSION','APP_USER'),SYS_CONTEXT('USERENV', 'SESSION_USER'));
    :new.creation_date    := current_timestamp;
    :new.last_updated_by  := COALESCE(SYS_CONTEXT('APEX$SESSION','APP_USER'),SYS_CONTEXT('USERENV', 'SESSION_USER'));
    :new.last_update_date := current_timestamp;
  ELSIF UPDATING THEN
    :new.last_updated_by  := COALESCE(SYS_CONTEXT('APEX$SESSION','APP_USER'),USER);
    :new.last_update_date := current_timestamp;
  END IF;
END;
/

SET DEFINE OFF;
INSERT INTO CNDEMO_TWTR_CAPTURE (CAPTURE_ID,QUERY_VALUE,DESCRIPTION,NAME,image_url)
  values (1,'(@oracleapex OR #orclapex)','Tweets for @OracleAPEX OR #orclAPEX','APEX','https://d4ozwpvxy0eb7.cloudfront.net/twitterstatsapp/APEX_Logo.webp');
INSERT INTO CNDEMO_TWTR_CAPTURE (CAPTURE_ID,QUERY_VALUE,DESCRIPTION,NAME,image_url)
  values (2,'(@oraclerest OR #ords)','Tweets for @OracleREST OR #ords','ORDS','https://d4ozwpvxy0eb7.cloudfront.net/twitterstatsapp/ORDS_Logo.webp');
  COMMIT;
