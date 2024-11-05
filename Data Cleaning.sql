-- Data Cleaning

SELECT * 
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null Values
-- 4. Remove Any Columns

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT * 
FROM layoffs_staging;

SELECT * ,
ROW_NUMBER () OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
( 
SELECT * ,
ROW_NUMBER () OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions ) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging
WHERE company = 'Casper';

WITH duplicate_cte AS
( 
SELECT * ,
ROW_NUMBER () OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions ) AS row_num
FROM layoffs_staging
)
DELETE  
FROM duplicate_cte 
WHERE row_num > 1;



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER () OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1 ;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1 ;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1 ;

SELECT * 
FROM layoffs_staging2;



-- Standardizing data

SELECT *
FROM layoffs_staging2
where industry LIKE 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

SELECT Distinct country
FROM layoffs_staging2
order by 1;

update layoffs_staging2
set country = 'United States'
where country like 'United States%';

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE (`date`, '%m/%d/%Y')

SELECT *
FROM layoffs_staging2
WHERE `date` = NULL;

UPDATE layoffs_staging2
SET `date` = NULL
WHERE `date` = 'NULL';

UPDATE layoffs_staging2
SET date = STR_TO_DATE(date, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off = 0 
AND percentage_laid_off = 0;

update layoffs_staging2
set industry = null
where industry = ''
or industry = "NULL";

select *
from layoffs_staging2
where industry is null
or industry = ''
;


select *
from layoffs_staging2
where company = 'Airbnb';


select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;   

select industry , company
from layoffs_staging2
where company = "Bally's Interactive";


select *
from layoffs_staging2
where industry = 'NULL';

delete
FROM layoffs_staging2
WHERE total_laid_off = 0 
AND percentage_laid_off = 0;

SELECT *
FROM layoffs_staging2;

alter table layoffs_staging2
drop column row_num; 

-- DONE
















