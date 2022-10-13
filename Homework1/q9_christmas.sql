SELECT group_concat(ProductName, ', ') 
FROM 
    (SELECT DISTINCT p.ProductName ProductName 
    FROM Product p 
    JOIN OrderDetail od 
    ON p.Id = od.ProductId 
    JOIN 'Order' o 
    ON o.Id = od.OrderId 
    JOIN Customer c 
    ON c.Id = o.CustomerId 
    WHERE c.CompanyName = 'Queen Cozinha' and o.OrderDate >= '2014-12-25' and o.OrderDate < '2014-12-26' 
    ORDER BY p.Id);