SELECT DISTINCT ShipName, substr(ShipName, 1, instr(ShipName, '-') - 1) 
FROM 'Order' 
WHERE ShipName LIKE '%-%' 
ORDER BY ShipName;
