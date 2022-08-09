--
-- Calculate a token's rarity score based on the total sum of it's trait's rarity percentage
--
UPDATE token SET rarity_score = b.rarity_score
FROM
(SELECT token_id, SUM(rarity_percentage) as rarity_score
FROM token_attribute 
GROUP BY token_id) b
WHERE token.token_id = b.token_id

--
-- If rarity_score is zero then set the value to zero because the token has no traits defined.
--
UPDATE token SET rarity_score = 0
WHERE rarity_score IS NULL

--
-- Rank the token's position within the collection based on it's rarity score
--
WITH tn as (
	SELECT t.token_id,
           ROW_NUMBER() OVER (PARTITION BY c.contract_id ORDER BY t.rarity_score DESC) rnk
	FROM token t
	INNER JOIN collection c ON t.contract_id = c.contract_id
)
UPDATE token
SET ranking = tn.rnk
FROM tn
WHERE token.token_id = tn.token_id