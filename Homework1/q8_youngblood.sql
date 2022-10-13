SELECT r.RegionDescription, e.FirstName, e.LastName, e.BirthDate 
FROM Region r 
JOIN Territory t 
ON t.RegionId = r.Id 
JOIN EmployeeTerritory et 
ON et.TerritoryId = t.Id 
JOIN Employee e 
ON et.EmployeeId = e.Id 
GROUP BY r.Id 
HAVING e.BirthDate = max(e.BirthDate) 
ORDER BY r.Id;