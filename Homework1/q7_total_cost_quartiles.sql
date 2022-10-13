
WITH t_expenditures AS (    
    SELECT DISTINCT ifnull(c.CompanyName, 'MISSING_NAME') companyName, o.CustomerId customerId, round(sum(od.UnitPrice * od.Quantity), 2) expenditure 
    FROM 'Order' o 
    LEFT JOIN Customer c 
    ON c.Id = o.customerId 
    JOIN OrderDetail od 
    ON o.Id = od.OrderId 
    GROUP BY o.CustomerId
), 
t_quartiles AS (
    SELECT *, NTILE(4) OVER (ORDER BY expenditure) quartile FROM t_expenditures
)
SELECT companyName, customerId, expenditure FROM t_quartiles WHERE quartile = 1 ORDER BY expenditure;
