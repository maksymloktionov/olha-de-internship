-- Write your PostgreSQL query statement below
WITH CTE_employee_salary_rank AS  
( 
SELECT d.name AS Department, e.name AS Employee,e.salary AS Salary ,
DENSE_RANK() OVER (PARTITION BY d.name ORDER BY e.salary DESC )AS salary_rank
FROM Employee AS e
INNER JOIN Department AS d
    ON e.departmentId = d.id
)

SELECT Department, Employee, Salary 
FROM CTE_employee_salary_rank
WHERE salary_rank <= 3