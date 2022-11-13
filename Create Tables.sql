Create Database SuperStoreDWH GO Use SuperStoreDWH2 GO Create Schema Staging GO Create Schema Core GO -- CREATE STAGING LAYER
Create Table Staging.Sales(
  RowID int primary key, 
  OrderID varchar(30), 
  OrderDate date, 
  ShipDate date, 
  ShipMode varchar(20), 
  CustomerID varchar(20), 
  CustomerName varchar(50), 
  Segment varchar(20), 
  Country varchar (100), 
  City varchar (100), 
  State varchar(100), 
  PostalCode varchar(10), 
  Region varchar(10), 
  ProductID varchar(20), 
  Category varchar(30), 
  SubCategory varchar(30), 
  ProductName varchar(300), 
  Sales Decimal (8, 2), 
  Quantity int, 
  Discount Decimal(3, 2), 
  Profit Decimal (8, 2)
) ------------------------------------------
-- CREATE DATE DIM
DECLARE @StartDate date = '20100101';
DECLARE @CutoffDate date = DATEADD(
  DAY, 
  -1, 
  DATEADD(YEAR, 30, @StartDate)
);
WITH CTE AS (
  SELECT 
    0 AS n, 
    CAST(@StartDate AS DATE) d 
  UNION ALL 
  SELECT 
    n + 1 AS n, 
    DATEADD(DAY, 1, d) as d 
  FROM 
    CTE 
  WHERE 
    n < DATEADD(
      DAY, 
      -1, 
      DATEDIFF(DAY, @StartDate, @CutoffDate)
    )
), 
DIM AS (
  SELECT 
    [Date_PK] = YEAR(d)* 10000 + MONTH(d)* 100 + DAY(d), 
    [Date] = CONVERT(DATE, d), 
    [Day] = DATEPART(DAY, d), 
    [DayName] = DATENAME(WEEKDAY, d), 
    [DayOfYear] = DATEPART(DAYOFYEAR, d), 
    [Week] = DATEPART(WEEK, d), 
    [DayOfWeek] = DATEPART(WEEKDAY, d), 
    [Month] = DATEPART(MONTH, d), 
    [MonthName] = DATENAME(MONTH, d), 
    [Quarter] = DATEPART(Quarter, d), 
    [Year] = DATEPART(YEAR, d), 
    [FirstOfMonth] = DATEFROMPARTS(
      YEAR(d), 
      MONTH(d), 
      1
    ), 
    [LastOfMonth] = EOMONTH(d), 
    [LastOfYear] = DATEFROMPARTS(
      YEAR(d), 
      12, 
      31
    ) 
  FROM 
    CTE
) 
SELECT 
  * INTO Core.Dim_Date 
FROM 
  DIM OPTION (MAXRECURSION 0);
ALTER TABLE 
  Core.Dim_Date ALTER COLUMN Date_PK INT NOT NULL;
ALTER TABLE 
  Core.Dim_Date 
Add 
  Constraint PK_DimDate_Date Primary Key (Date_PK) 
  
  ------------------------------------------
  -- CREATE CUSTOMER DIM
  CREATE Table Core.Dim_Customer(
    Customer_PK int primary key identity(1, 1), 
    CustomerID varchar(20), 
    FirstName varchar(50), 
    LastName varchar(50), 
    Segment varchar(20)
  ) ------------------------------------------
  -- CREATE PRODUCT DIM
  CREATE Table Core.Dim_Product(
    Product_PK int primary key identity(1, 1), 
    ProductID varchar(20), 
    Category varchar(30), 
    SubCategory varchar(30), 
    ProductName varchar(300)
  ) ------------------------------------------
  -- CREATE LOCATION DIM
  Create Table Core.Dim_Location(
    Location_PK int primary key identity(1, 1), 
    City varchar (100), 
    State varchar(100), 
    PostalCode varchar(10), 
    Region varchar(10)
  ) ------------------------------------------
  -- CREATE SALES FACT
  Create Table Core.Fact_Sales(
    RowID int primary key, 
    OrderDate_FK int foreign key references Core.Dim_Date(Date_PK), 
    ShipDate_FK int foreign key references Core.Dim_Date(Date_PK), 
    ShipMode_DD varchar(20), 
    Customer_FK int foreign key references Core.Dim_Customer(Customer_PK), 
    Location_FK int foreign key references Core.Dim_Location(Location_PK), 
    Product_FK int foreign key references Core.Dim_Product(Product_PK), 
    Cost Decimal (8, 2), 
    Price Decimal (8, 2), 
    Discount Decimal (8, 2), 
    FinalPrice Decimal (8, 2), 
    Quantity int, 
    Sales Decimal (8, 2), 
    Profit Decimal (8, 2)
  ) -----------------------------------------------
  
  