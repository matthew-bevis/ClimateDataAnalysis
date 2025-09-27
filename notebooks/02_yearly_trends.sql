-- Databricks notebook source
SELECT substr(date, 1, 4) AS year,
       AVG(lai)   AS avg_lai,
       AVG(fapar) AS avg_fapar
FROM climate
GROUP BY year
ORDER BY year;
