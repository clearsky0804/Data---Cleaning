-- Start transaction for data integrity
START TRANSACTION;

-- Create a staging table with the same structure as the 'layoffs' table
CREATE TABLE layoffstaging LIKE layoffs;

-- Insert all data from 'layoffs' into 'layoffstaging'
INSERT INTO layoffstaging
SELECT * FROM layoffs;

-- Create layoffstaging2 table with additional row_num column
CREATE TABLE `layoffstaging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Insert data into layoffstaging2 with row numbers
INSERT INTO layoffstaging2
SELECT *,
 ROW_NUMBER() OVER (
               PARTITION BY 
                   company, 
                   location, 
                   industry, 
                   total_laid_off, 
                   percentage_laid_off, 
                   date, 
                   stage, 
                   country, 
                   funds_raised_millions
           )
FROM layoffstaging;

-- Remove duplicate rows
DELETE FROM layoffstaging2 WHERE row_num > 1;

-- Standardizing the data
UPDATE layoffstaging2
SET 
    company = TRIM(company),
    industry = CASE 
        WHEN industry LIKE 'Crypto %' THEN 'Crypto'
        ELSE industry
    END,
    country = TRIM(TRAILING '.' FROM country),
    date = STR_TO_DATE(date, '%m/%d/%Y')
WHERE 
    company IS NOT NULL OR
    industry LIKE 'Crypto %' OR
    country LIKE '% United State%' OR
    date IS NOT NULL;

-- Update NULL industries
UPDATE layoffstaging2 t1
JOIN (
    SELECT company, location, MAX(industry) AS industry
    FROM layoffstaging2
    WHERE industry IS NOT NULL AND industry != ''
    GROUP BY company, location
) t2 ON t1.company = t2.company AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL OR t1.industry = '';

-- Delete rows with NULL total_laid_off and percentage_laid_off
DELETE FROM layoffstaging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Alter table to change date column type and drop row_num
ALTER TABLE layoffstaging2
MODIFY COLUMN `date` DATE,
DROP COLUMN row_num;

-- Add indexes for frequently queried columns
ALTER TABLE layoffstaging2
ADD INDEX idx_company (company(255)),
ADD INDEX idx_industry (industry(255)),
ADD INDEX idx_country (country(255)),
ADD INDEX idx_date (date);

-- Commit the transaction
COMMIT;

-- Analysis queries

-- Companies with most layoffs
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM layoffstaging2
GROUP BY company
ORDER BY total_layoffs DESC
LIMIT 10;

-- Industries with most layoffs
SELECT industry, SUM(total_laid_off) AS total_layoffs
FROM layoffstaging2
GROUP BY industry
ORDER BY total_layoffs DESC
LIMIT 10;

-- Countries with most layoffs
SELECT country, SUM(total_laid_off) AS total_layoffs
FROM layoffstaging2
GROUP BY country
ORDER BY total_layoffs DESC
LIMIT 10;

-- Layoffs by year
SELECT YEAR(date) AS year, SUM(total_laid_off) AS total_layoffs
FROM layoffstaging2
GROUP BY year
ORDER BY year DESC;

-- Layoffs by stage
SELECT stage, SUM(total_laid_off) AS total_layoffs
FROM layoffstaging2
GROUP BY stage
ORDER BY total_layoffs DESC;

-- Monthly layoffs with rolling total
WITH monthly_layoffs AS (
    SELECT 
        DATE_FORMAT(date, '%Y-%m') AS Month,
        SUM(total_laid_off) AS totaloff
    FROM layoffstaging2
    GROUP BY Month
    ORDER BY Month
)
SELECT 
    Month,
    totaloff,
    SUM(totaloff) OVER (ORDER BY Month) AS rolling_total
FROM monthly_layoffs;

-- Optional: Create a stored procedure for updating NULL industries
DELIMITER //
CREATE PROCEDURE update_null_industries()
BEGIN
    UPDATE layoffstaging2 t1
    JOIN (
        SELECT company, location, MAX(industry) AS industry
        FROM layoffstaging2
        WHERE industry IS NOT NULL AND industry != ''
        GROUP BY company, location
    ) t2 ON t1.company = t2.company AND t1.location = t2.location
    SET t1.industry = t2.industry
    WHERE t1.industry IS NULL OR t1.industry = '';
END //
DELIMITER ;

-- Call the stored procedure if needed
-- CALL update_null_industries();
