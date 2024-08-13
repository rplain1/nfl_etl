WITH ids AS (
    SELECT gsis_id, merge_name, sleeper_id, position, pfr_id,
    CASE
        WHEN merge_name = 'mike williams' AND gsis_id != '00-0033536' THEN FALSE
        WHEN merge_name = 'steve smith' AND gsis_id != '00-0020337' THEN FALSE
        WHEN merge_name = 'zach miller' AND gsis_id != '00-0027125' THEN FALSE
    ELSE TRUE END AS filter_var
    FROM BASE.IDS
),

adp AS (
    SELECT *,
    CASE
        WHEN name = 'Mike Vick' THEN 'michael vick'
        WHEN name like ' Jr' THEN lower(regexp_replace(name, ' Jr', ''))
    ELSE lower(name) END AS join_name
    FROM BASE.FFC_ADP_PPR
)

SELECT adp.*
, sleeper_id
, pfr_id
, gsis_id
, row_number() OVER (PARTITION BY adp.season, adp.position ORDER BY adp.adp) AS pos_rank
FROM adp
LEFT JOIN (select * from ids where filter_var = TRUE) ids
ON ids.merge_name = adp.join_name and ids.position = adp.position
