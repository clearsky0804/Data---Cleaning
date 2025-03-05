DROP TABLE layoffs;

CREATE table layoffs(
company VARCHAR(128),
location VARCHAR (128),
industry VARCHAR(128),
total_laid_off INTEGER DEFAULT NULL,
percentage_laid_off TEXT,
date VARCHAR(64),
stage VARCHAR(64),
country VARCHAR(64),
funds_raised_millions NUMERIC(8,2)
);

DROP TABLE layoffstaging;

CREATE TABLE layoffstaging (LIKE layoffs INCLUDING ALL);


INSERT INTO layoffstaging
SELECT * FROM layoffs;

DROP TABLE layoffstaging2;

CREATE TABLE layoffstaging2 (
company VARCHAR(128),
location VARCHAR (128),
industry VARCHAR(128),
total_laid_off INTEGER DEFAULT NULL,
percentage_laid_off TEXT,
date VARCHAR(64),
stage VARCHAR(64),
country VARCHAR(64),
funds_raised_millions NUMERIC(8,2),
row_num INTEGER
);

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

DELETE FROM layoffstaging2 WHERE row_num > 1;  -- 5 fake entries were deleted


UPDATE layoffstaging2   -- standardizing the data
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
    industry LIKE 'Crypto %' OR
    country LIKE '% United State%' OR
    date IS NOT NULL;



UPDATE layoffstaging2 t1   -- 3 entries where the industry was ''
SET industry = t2.industry
FROM (
    SELECT company, location, MAX(industry) AS industry
    FROM layoffstaging2
    WHERE industry IS NOT NULL AND industry != ''
    GROUP BY company, location
) t2 
WHERE t1.company = t2.company AND t1.location = t2.location
  AND (t1.industry IS NULL OR t1.industry = '');

  -- Delete rows with NULL total_laid_off and percentage_laid_off
DELETE FROM layoffstaging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;



-- Alter table to change date column type and drop row_num
ALTER TABLE layoffstaging2
ALTER COLUMN date TYPE DATE USING date::DATE,
DROP COLUMN row_num;

CREATE INDEX idx_company ON layoffstaging2 (company);
CREATE INDEX idx_industry ON layoffstaging2 (industry);
CREATE INDEX idx_country ON layoffstaging2 (country);
CREATE INDEX idx_date ON layoffstaging2 (date);

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
SELECT EXTRACT(YEAR FROM date) AS year, SUM(total_laid_off) AS total_layoffs
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
    	TO_CHAR(date, 'YYYY-MM') AS Month,
    	SUM(total_laid_off) AS totaloff
	FROM layoffstaging2
	GROUP BY TO_CHAR(date, 'YYYY-MM')
	ORDER BY Month
)
SELECT 
    Month,
    totaloff,
    SUM(totaloff) OVER (ORDER BY Month) AS rolling_total
FROM monthly_layoffs;







