SELECT datetime, COUNT(*) as cnt
FROM "ES"
GROUP BY datetime
HAVING COUNT(*) > 1
LIMIT 10;
