SELECT c.CategoryName, count(*), round(avg(p.UnitPrice), 2), min(p.UnitPrice), max(p.UnitPrice), sum(p.UnitsOnOrder) 
FROM Category c 
JOIN Product p 
ON c.Id = p.CategoryId 
GROUP BY c.Id 
HAVING count(*) > 10 
ORDER BY p.CategoryId;