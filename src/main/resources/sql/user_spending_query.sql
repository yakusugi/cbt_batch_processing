select * from user_spending 
where email = '%s' 
and currency_code = '%s' 
or currency_code = '%s' 
order by spending_id
