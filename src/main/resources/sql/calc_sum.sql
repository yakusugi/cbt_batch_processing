SELECT *
FROM user_spending
WHERE spending_date BETWEEN '%s' AND '%s'
AND currency_code = '%s'
AND email = '%s'