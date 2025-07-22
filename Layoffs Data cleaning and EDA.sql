#Data cleaning

#viewing the Data 
select * from World_Layoffs.layoffs;

-- 1.Remove Duplicates
-- 2. standartize the data
-- 3. remove null values or blank values
-- 4. remove columns which are unnecesary

#Here, I copied the old data to new data  to make sure that none of the columns or rows will be deleted from the original data set.
Create table World_Layoffs.layoffs_staging
like World_Layoffs.layoffs;

#After staging viewing the data 
select * 
from World_Layoffs.layoffs_staging;

#The table it self will copy the data but not inside the table.
# so, I am inserting the data from original data to new duplicated data where almost all the changes will be done
Insert World_Layoffs.layoffs_staging
select * 
from World_Layoffs.layoffs;
#Now checking if the data is copied or not.
select * 
from World_Layoffs.layoffs_staging;

-- 1.Remove Duplicates
#To remove the duplicates i created a row_number 
select * ,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`)as row_num
from World_Layoffs.layoffs_staging;

#Here, I created a CTE and asked to show me if there is any row_numbers are greater than 1.

WITH duplicate_cte AS
(
select * ,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions)as row_num
from World_Layoffs.layoffs_staging
)
select * 
from duplicate_cte
where row_num > 1;

#I created another data table from the old table where i can include the row_number.
CREATE TABLE `layoffs_staging2` (
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

#Viewing the data where the row_num are greater than 1 
select * 
from layoffs_staging2
Where row_num >1;

# Here, inserting the data for second staging where i can include the row_number and i did the partition
Insert into layoffs_staging2
select * ,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions)as row_num
from World_Layoffs.layoffs_staging;

#Once, i am done doing the insertion i delete the rows where the row_num is greater than 1
delete 
from layoffs_staging2
Where row_num >1;

#Checking if there is any rows that are greater than 1
select * 
from layoffs_staging2
Where row_num >1;

select * 
from layoffs_staging2;

-- 2. standartize the data
#Removing the whitespaces for the company column
select company, trim(company)
from layoffs_staging2;

#updating the original column after the whitespaces removed.
UPDATE layoffs_staging2
set company = trim(company);

#Checking the distinct companies
select distinct(Industry)
from layoffs_staging2
order by 1;
#Here, the crypto was written in a different way. viewed the data with all the different words of crypto
select * 
from layoffs_staging2
where industry like 'crypto%';
#updated the all different crypto words to one common word crypto
update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

#Checking the distinct location
select DISTINCT Location
from layoffs_staging2
ORder by 1;

#For this particular location i can see the . so, i wanted to remove that and update it
select DISTINCT country, trim(trailing'.' from country)
from layoffs_staging2
ORder by 1;

UPDATE layoffs_staging2
set country = trim(trailing'.' from country)
where country like 'united states%';

#Date type is in text and we need to convert that to date
#so, first changed to date format for all the dates 
select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;
#updated the date format as well for staging2
UPDATE layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

#Then, altered the stagings2 data with data type
alter table layoffs_staging2
modify  column `date` DATE;

select * 
from layoffs_staging2;

-- 3. remove null values or blank values
#Most common nulls combinations are these two which mentioned below
select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;
#There are the columns where the industry is null and blank
select * 
from layoffs_staging2
where industry is null or
industry = '';
#checked with companies individually 
select *
from layoffs_staging2
where company = 'Airbnb';
#Here,I  used joins for the single table showig the industry is null or blank and industry is not null
select *
from layoffs_staging2 t1 
join layoffs_staging2 t2
on t1.company = t2.company
and t2.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null; 

#update the staging2 data table where industry is null and not null
UPDATE layoffs_staging2  t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null; 

#The data is mostly related with layoffs. I couldn't see the info on total_laid_off and percentage_laid_off. where, there is no information on how many employees were there in the company.
#so, deleted the rows for the columns of total_laid_off and percentage_laid_off
delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- 4. Removing the column which is unnecesary
#Unnecessary column here is row_num and delete it because we will not use for future purpose.
alter table layoffs_staging2
drop  column row_num;

-- #Exploratory Data Analysis 

-- Here we are just going to explore the data and find trends or patterns or anything interesting like outliers

-- normally when you start the EDA process you have some idea of what you're looking for

-- with this info we are just going to look around and see what we find!

select *
from layoffs_staging2;

#checking the max total_laid_off and percentage_laid_odd
select Max(total_laid_off),max(percentage_laid_off)
from layoffs_staging2;

#checking if there is any company percentage_laid_off is 100% and seeing the total_laid_off from descending
select *
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

#checking the company and sum of the total layoffs where i group by company 
select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc; 

#seeing the max and min date of this data
select min(`date`),max(`date`)
from layoffs_staging2;

#checking the industry and sum of the total layoffs where i group by industry
select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc; 

#checking the country and sum of the total layoffs where i group by country
select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc; 

#checking the year(date) and sum of the total layoffs where i group by date
select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc; 
#checking the stage and sum of the total layoffs where i group by stage
select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 1 desc; 

#checking the company and sum of the total layoffs where i group by company 
select company, sum(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc; 

#Rolling Layoffs per month
select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;

#Here, i tried to take the data from year to month and total sum of layoffs and where dates are not null and ordered by asc.
#I used window function for the rolling total for each month 
with rolling_total as
(
select substring(`date`,1,7) as `month`, sum(total_laid_off)as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`,total_off,
 sum(total_off) over(order by `month`) as rolling_total
from rolling_total;

-- Layoff total per year
select company, year(date),sum(total_laid_off )
from layoffs_staging2
group by company,year(date)
order by 3 desc; 

#Here, i wanted to know the total_laid_off per year from each company . so, i created a couple of CTE's where in 1st CTE i mentioned about the data year and totallayofss and company
#where i group the data by company and year
#In the second CTE i created a window function using dense_rank and partitioning the data by year and ordered by total layoffs desc and mentioned data that years is not null
# we just want to see the rank_num only for the top 5 companies.
with company_year(company,years,total_laid_off) as
(
select company, year(date),sum(total_laid_off )
from layoffs_staging2
group by company,year(date)
order by 3 desc
) , company_year_rank as 
(
select * ,dense_rank() over(partition by years order by total_laid_off desc) as rank_num
from company_year
where years is not null 
)
select *
from company_year_rank
where rank_num <= 5;
