-- Write your PostgreSQL query statement below
WITH CTE_emp AS  
( 
SELECT d.name AS Department, e.name AS Employee,e.salary AS Salary ,
DENSE_RANK() OVER (PARTITION BY d.name ORDER BY e.salary DESC )AS rn
FROM Employee AS e
INNER JOIN Department AS d
    ON e.departmentId = d.id
)

SELECT Department, Employee, Salary 
FROM CTE_emp
WHERE rn BETWEEN 1 AND 3