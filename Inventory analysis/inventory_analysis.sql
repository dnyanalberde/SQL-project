-- Total inventory available in stock
SELECT SUM(quantity) AS total_inventory
FROM Inventory_analysis.stock;

-- Each warehouse total capacity
SELECT war_name, SUM(capacity) AS total_capacity
FROM Inventory_analysis.warehouse
GROUP BY war_name;

-- To find all the names of the employees with designations as 'Stock Manager'
SELECT emp_name, designation 
FROM Inventory_analysis.employee
WHERE designation = 'Stock Manager'
GROUP BY emp_name;

-- Average price of all the products
SELECT ROUND(AVG(price), 2) AS avg_price 
FROM Inventory_analysis.product;

-- Using Case statement to get the designation of the Employees
SELECT emp_name, designation 
FROM inventory_analysis.employee
ORDER BY (
CASE 
WHEN designation IS NULL
THEN emp_name
ELSE designation
END);

-- To find the amount of Dairy products that are ordered
SELECT DISTINCT COUNT(type) 
OVER (PARTITION BY prod_name ORDER BY type DESC) AS no_of_products,prod_name,type 
FROM  Inventory_analysis.product
WHERE type = 'Dairy'
ORDER BY no_of_products DESC;

-- To find the orders returned by Customers due to damage
SELECT Inventory_analysis.order.order_id, Inventory_analysis.return.reason 
FROM Inventory_analysis.order
JOIN inventory_analysis.return
ON Inventory_analysis.order.order_id = inventory_analysis.return.order_id
WHERE Inventory_analysis.return.reason = 'Damaged'
AND Inventory_analysis.order.order_type = 'Customer';

--  Number of different products stock in the inventory
SELECT Inventory_analysis.product.prod_name, 
SUM(Inventory_analysis.stock.quantity) AS Total_quantity
FROM inventory_analysis.product
JOIN Inventory_analysis.stock
ON inventory_analysis.product.prod_id = Inventory_analysis.stock.prod_id
GROUP BY Inventory_analysis.product.prod_name
HAVING COUNT(Inventory_analysis.stock.quantity) > 1;

