SELECT p.ProductName, c.CompanyName, c.ContactName 
FROM Product p 
JOIN OrderDetail od 
ON p.Id = od.ProductId 
JOIN 'Order' o 
ON od.OrderId = o.Id 
JOIN Customer c 
ON o.CustomerId = c.Id 
WHERE p.Discontinued = 1 
GROUP BY p.Id 
HAVING o.orderDate = min(o.orderDate) 
ORDER BY p.ProductName;