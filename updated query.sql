 -- Step 1 Remove Duplicates
-- Step 2 Standardize DATA
-- Step 3 Update NULLS if possible with the available DATA
-- Step 4 Remove unnecessary COLUMNS

START TRANSACTION;

DROP TABLE IF EXISTS layoffstaging;

CREATE TABLE layoffstaging (LIKE layoffs INCLUDING ALL);
INSERT INTO layoffstaging
SELECT *
FROM layoffs;

-- Step 1 removing duplicates

-- We cannot use CTEs to update table, CTEs can only show 
DROP TABLE IF EXISTS layoffstaging_2;


CREATE table layoffstaging_2(
company VARCHAR(128),
location VARCHAR (128),
industry VARCHAR(128),
total_laid_off INTEGER DEFAULT NULL,
percentage_laid_off NUMERIC(3,2),
date VARCHAR(64),
stage VARCHAR(64),
country VARCHAR(64),
funds_raised_millions NUMERIC(8,2),
rownum INTEGER
);

INSERT INTO layoffstaging_2 
SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY
						company,
						location,
						industry,
						total_laid_off,
						percentage_laid_off,
						date,
						stage,
						country,
						funds_raised_millions)
FROM layoffstaging;

-- DELETING DUPLICATES

DELETE
FROM layoffstaging_2
WHERE rownum >1;

-- Step 2 Standardizing DATA (check all fields and identify the problems)
-- problems identified 1) there are multiple crypto in the industry, united states has a '.' at the end making it different, trim on company name extra spaces

 
ALTER TABLE layoffstaging_2 
DROP COLUMN rownum;

UPDATE layoffstaging_2
SET 
	company = TRIM(company),
	    industry = CASE 
        			WHEN industry LIKE 'Crypto %' THEN 'Crypto'
        			ELSE industry
    			END,
	country = TRIM(TRAILING '.' FROM country),
	date = TO_DATE(date, 'MM/DD/YY')
WHERE 
	company IS NOT NULL OR
	industry LIKE 'Crypto%' OR
	country LIKE 'United States%' OR
	DATE IS NOT NULL ;

-- Convert the date column data type to the right one

ALTER TABLE layoffstaging_2
ALTER COLUMN date type DATE using date::DATE ;


-- STEP 3 Updating NULLS
-- There are nulls in industry which can be filled with the data available
UPDATE layoffstaging_2 AS t1
	SET industry = t2.industry
FROM 
	(
		SELECT 
			company,
			location,
			MAX(industry) AS industry
		FROM layoffstaging_2	
		WHERE industry <> '' AND industry IS NOT NULL
		GROUP BY company, location
	) AS t2
WHERE t1.industry ='' AND t2.industry IS NOT NULL;

-- STEP 4 DELETING ROWS where we cannot do analysis

DELETE 
FROM layoffstaging_2 
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

COMMIT;

-- Analysis of the data starts

-- 1) WHat is the timespan of the data
SELECT
	MIN(date),
	MAX(date)
FROM layoffstaging_2;

-- 2) What is the maximum number that has been laid off in a single layoff and the percentage of people they laid OFF
SELECT 
	MAX(total_laid_off),
	MAX(percentage_laid_off)
FROM layoffstaging_2;

-- There are instances where all the employees of the company have been laid off

-- Company wise basic analysis
SELECT
	company,
	SUM(total_laid_off) AS sum
FROM layoffstaging_2
GROUP BY company
HAVING sum(total_laid_off) IS NOT NULL
ORDER BY 2 DESC ;

-- Industry wise basic analysis
SELECT
	industry,
	SUM(total_laid_off) AS sum
FROM layoffstaging_2
GROUP BY industry
HAVING sum(total_laid_off) IS NOT NULL
ORDER BY 2 DESC ;

-- Consumer industry was hit the most


--Location based analysis
SELECT
	location,
	SUM(total_laid_off) AS sum
FROM layoffstaging_2
GROUP BY location
HAVING sum(total_laid_off) IS NOT NULL
ORDER BY 2 DESC ;
-- Bay Area was hit the most

-- country based analysis
SELECT
	country,
	SUM(total_laid_off) AS sum
FROM layoffstaging_2
GROUP BY country
HAVING sum(total_laid_off) IS NOT NULL
ORDER BY 2 DESC ;
--United states was affected the most

-- Timeseries of layoffs on YEAR
SELECT
	Extract(year from date),
	SUM(total_laid_off) AS sum
FROM layoffstaging_2
GROUP BY Extract(year from date)
HAVING sum(total_laid_off) IS NOT NULL
ORDER BY 1 DESC ;

--ROLLING layoffs
WITH rolling_total AS (
SELECT SUBSTRING(date::text , 1, 7 ) AS MONTH,
		SUM(total_laid_off) AS total_off
FROM layoffstaging_2
WHERE SUBSTRING(date::text , 1, 7 ) IS  NOT NULL
GROUP BY MONTH
)

SELECT
	month,
	total_off,
	SUM(total_off) OVER (ORDER BY month) AS rolling_total
FROM rolling_total;


--  Top 5 companies with most layoffs
WITH company_year AS (
SELECT	
	company,
	SUBSTRING(date:: text, 1, 7) AS year_date,
	SUM(total_laid_off) AS total_off
FROM 
	layoffstaging_2
GROUP BY company, SUBSTRING(date::text, 1, 7) 
ORDER BY 3 DESC
),
per_year_stats AS (
SELECT *,
	DENSE_RANK() OVER(PARTITION BY year_date ORDER BY total_off DESC) AS ranking
FROM company_year
WHERE total_off IS NOT NULL
ORDER BY year_date 
)

SELECT *
FROM per_year_stats
WHERE ranking <=5;

