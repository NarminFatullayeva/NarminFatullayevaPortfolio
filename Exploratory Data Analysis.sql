-- Exploratory Data Analysis

select * 
from layoffs_staging2;

select MAX(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

update layoffs_staging2
set total_laid_off = null
where total_laid_off = 'NULL'
or total_laid_off = 0 ;

update layoffs_staging2
set funds_raised_millions = null
where funds_raised_millions = 'NULL'
or funds_raised_millions = 0 ;

update layoffs_staging2
set fund = null
where percentage_laid_off = 'NULL'
or percentage_laid_off = 0 ;

ALTER TABLE layoffs_staging2
MODIFY COLUMN total_laid_off INT;

ALTER TABLE layoffs_staging2
MODIFY COLUMN funds_raised_millions INT;

select * 
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select industry, sum(total_laid_off)
from layoffs_staging2
group by  industry
order by 2 desc;

select min(`date`), max(`date`)
from layoffs_staging2;

select country, sum(total_laid_off)
from layoffs_staging2
group by  country
order by 2 desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by  year(`date`)
order by 2 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by  stage
order by 2 desc;

select company, avg(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;


select substring(`date`, 1, 7) as `month`, sum(total_laid_off)
from layoffs_staging2
where  substring(`date`, 1, 7) is not null
group by `month`
order by 1 asc;

with rolling_total as 
(select substring(`date`, 1, 7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where  substring(`date`, 1, 7) is not null
group by `month`
order by 1 asc
)
select `month`, total_off, sum(total_off) over(order by `month`)
from rolling_total;

select company,year(`date`), sum(total_laid_off)
from layoffs_staging2
group by  company, year(`date`)
order by 3 desc;

with company_year (company, years, total_laid_off) as 
(
select company,year(`date`), sum(total_laid_off)
from layoffs_staging2
group by  company, year(`date`)
), company_years_rank as
(select *, 
dense_rank() over ( partition by years order by total_laid_off desc ) as ranking
from company_year
where years is not null
)
select *
from company_years_rank
where ranking <= 5;














