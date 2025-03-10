-- Data Cleaning
select * from layoffs;

create table layoffs_staging like layoffs;
insert layoffs_staging
select * from layoffs;

select * from layoffs_staging;
select distinct company from layoffs_staging;
select distinct location from layoffs_staging;
select distinct industry from layoffs_staging;
select distinct country from layoffs_staging;

select *,
row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;

-- using cte for finding duplicates
with cte as
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) as row_num
from layoffs_staging
)
select * 
from cte
where row_num>1;

-- looking for a specific duplicate
select * from layoffs_staging
where company='Cazoo';

-- creating a table with a column filled with number of number of the same information 
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` bigint DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoffs_staging2;

-- insering data into new table
insert into layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) as row_num
from layoffs_staging;

select * 
from layoffs_staging2
where row_num>1;

-- deleting duplicate data
delete
from layoffs_staging2
where row_num> 1 ;

-- Standardizing data
select * from layoffs_staging2;
select distinct company from layoffs_staging2;
select distinct location from layoffs_staging2;
select distinct industry from layoffs_staging2;
select distinct country from layoffs_staging2;

-- removing empty space before the text
update layoffs_staging2
set company = trim(company);

update layoffs_staging2
set location = trim(location);

update layoffs_staging2
set industry = trim(industry);

update layoffs_staging2
set total_laid_off = trim(total_laid_off);

update layoffs_staging2
set percentage_laid_off = trim(percentage_laid_off);

update layoffs_staging2
set `date` = trim(`date`);

update layoffs_staging2
set stage = trim(stage);

update layoffs_staging2
set country = trim(country);

update layoffs_staging2
set funds_raised = trim(funds_raised);

-- updating industry where their name is 'Crypto...' to 'Crypto'
select industry
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry='Crypto'
where industry like 'Crypto%';

-- updating country where their name is 'United States.' to 'United States'
select distinct country
from world_layoffs.layoffs_staging2;

update layoffs_staging2
set country = TRIM(trailing '.' from country);

-- fixing date type 
update layoffs_staging2
set `date` = DATE_FORMAT(STR_TO_DATE(`date`, '%Y-%m-%dT%H:%i:%s.000Z'), '%m/%d/%Y');

-- deleting data where industry is not present
select * 
from layoffs_staging2
where company = 'Appsmith';

delete 
from layoffs_staging2
where industry = '';

-- deleting data where stage is not present
select * 
from layoffs_staging2
where company = 'Advata';

-- removing row_num column
alter table layoffs_staging2
drop column row_num;

delete 
from layoffs_staging2
where stage = '';
