--
-- Add the below changes to the tables based on 8/3 group discussion
--
ALTER TABLE data_analysis ADD whale_ratio NUMERIC;
ALTER TABLE token ADD rarity_score NUMERIC;
ALTER TABLE token ADD ranking INTEGER;
ALTER TABLE social_media ADD hash_tag VARCHAR;

ALTER TABLE contract RENAME TO collection;


