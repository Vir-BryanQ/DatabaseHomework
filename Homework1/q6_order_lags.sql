SELECT Id, OrderDate, PrevOrderDate, round(julianday(OrderDate) - julianday(PrevOrderDate), 2) 
FROM 
(SELECT Id, OrderDate, lag(OrderDate, 1, OrderDate) OVER (ORDER BY OrderDate) PrevOrderDate
FROM 'Order'
WHERE CustomerId = 'BLONP'
ORDER BY OrderDate
LIMIT 10)



-- implementation without using lag() function

/*SELECT o1.Id, o1.OrderDate, o2.OrderDate, round(julianday(o1.OrderDate) - julianday(o2.OrderDate), 2) 
FROM (SELECT * FROM 'Order' WHERE CustomerId = 'BLONP' ORDER BY OrderDate LIMIT 10) o1 
JOIN (SELECT * FROM 'Order' WHERE CustomerId = 'BLONP' ORDER BY OrderDate LIMIT 10) o2 
ON o2.OrderDate < o1.OrderDate GROUP BY o1.Id HAVING o2.OrderDate = max(o2.OrderDate) 
UNION 
SELECT Id, OrderDate, OrderDate, 0.0 
FROM 'Order' 
WHERE CustomerId = 'BLONP' 
GROUP BY null 
HAVING OrderDate = min(OrderDate) 
ORDER BY o1.OrderDate;*/