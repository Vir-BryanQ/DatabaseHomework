SELECT s.CompanyName, round(count(o.ShippedDate > o.RequiredDate or null) * 100.0 / count(*), 2) percentage 
FROM Shipper s 
JOIN 'Order' o 
ON s.Id = o.ShipVia 
GROUP BY s.Id 
ORDER BY percentage desc;