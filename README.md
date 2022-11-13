# SuperStore-DWH
##### Case Study for Creating a Data Warehouse and ETL Job

## Table Of Contents
- [Problem Description](#problem)
- [Prerequisite](#pre) 
- [Implementation](#imp)
- [Result](#res)

<a name="problem"></a>
## Problem Description

- Design and implement a data warehouse (Star Schema).
- Extract, Transform and Load data from the source data in `CSV` format into the DWH. It contains Sales & Profits of a Superstore, and can be found on [`Kaggle`](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final).
- Creat a simple Report/Dashboard answering business questions.


<a name="pre"></a>
## Prerequisite
- DWH Concepts and Dimensional Modeling
- SQL (MS SQL Server)
- Data Integration Tool (Pentaho)
- Data Visualization Tool (Power BI)


<a name="imp"></a>
## Implementation
1. [Look at the Problem and Plan](#1)
1. [Create Staging Layer Table](#2)
1. [Create Core Layer Tables](#3)
    1. Dimension Tables
    1. Fact Table 
1. [Setup ETL Job](#4)

<a name="1"></a>
### Look at the Problem and Plan
     
Source data comes in `CSV` file and contains example sales transaction with a header:

|Row ID|Order ID|Order Date|Ship Date|Ship Mode|Customer ID|Customer Name|Segment|Country|City|State|Postal Code|Region|Product ID|Category|Subcategory|Product Name|Sales|Quantity|Discount|Profit|
|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|

##### Metadata Description:

**Row ID**: Unique ID for each row.

**Order ID**: Unique Order ID for each Customer.

**Order Date**: Order Date of the product.

**Ship Date**: Shipping Date of the Product.

**Ship Mode**:  Shipping Mode specified by the Customer.

**Customer ID**: Unique ID to identify each Customer.

**Customer Name**: Name of the Customer.

**Segment**: The se**gment where the Customer belongs.

**Country**: Country of residence of the Customer.

**City**: City of residence of the Customer.

**State**: State of residence of the Customer.

**Postal Code**: Postal Code of every Customer.

**Region**: Region where the Customer belongs.

**Product ID**: Unique ID of the Product.

**Category**: Category of the product ordered.

**Subcategory**: Subcategory of the product ordered.

**Product Name**: Name of the Product.

**Sales**: Sales of the Product.

**Quantity**: Quantity of the Product.

**Discount**: Discount provided.

**Profit**: Profit/Loss incurred.

#### And we want to answer some business questions:
- Which Category is Best Selling and Most Profitable?
- What are the Best Selling and Most Profitable Sub-Category?
- Which Customer Segment is Most Profitable?
- Which is the Preferred Ship Mode?
- Which Region is the Most Profitable?
- Which City has the Highest Number of Sales?



After analyzing source data and business requirments, our star schema will be like:

![Star Schema](https://user-images.githubusercontent.com/47898196/201498250-77d228b4-2595-4761-9efa-71d022230ffa.png)


##### Notes

- Surrogate key used instead of natural keys in `Dim_Product` and `Dim_Customer`, it gives better performance.
- We not intersted in `Order Id` in this case because our grain is order line, but it can be kept in `Dim_Order`if we need in another cases.
- `ShipMode` is handeled in that case as a Degenerate Dimension, but it can be grouped with `ShipDate` in one dimension `Dim_Ship` and in this case Outrigger Dimension will appear because this `Dim_Ship` wii refrence the `Dim_Date`.


<a name="2"></a>
### Create Staging Layer Table
First we need to create a database object for our DWH and Schema object for our layers:

```sql
Create Database SuperStoreDWH
Use SuperStoreDWH
GO
Create Schema Staging
GO
Create Schema Core

```
Second is create Staging Layer into which source data will be extracted:
```sql
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
)
```

<a name="3"></a>
### Create Core Layer Tables

First we will implement our dimension tables

```sql
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
  ) 
  
  ------------------------------------------
  -- CREATE PRODUCT DIM
  CREATE Table Core.Dim_Product(
    Product_PK int primary key identity(1, 1), 
    ProductID varchar(20), 
    Category varchar(30), 
    SubCategory varchar(30), 
    ProductName varchar(300)
  ) 
  
  ------------------------------------------
  -- CREATE LOCATION DIM
  Create Table Core.Dim_Location(
    Location_PK int primary key identity(1, 1), 
    City varchar (100), 
    State varchar(100), 
    PostalCode varchar(10), 
    Region varchar(10)
  ) ------------------------------------------

```

Then will implement our final target table (Fact_Sales)

```sql
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
)

```
Now our Star Schema is ready to start our ETL Job


<a name="4"></a>
### Setup ETL Job

Using pentaho to create a simple ETL Job

![ETL Job](https://user-images.githubusercontent.com/47898196/201500515-dd684959-e126-49ee-9b8c-827732f13ed2.png)

<a name="res"></a>
### Result

Using Power BI to create simple report/dashboard answering buisness questions of:
- Which Category is Best Selling and Most Profitable?
- What are the Best Selling and Most Profitable Sub-Category?
- Which Customer Segment is Most Profitable?
- Which is the Preferred Ship Mode?
- Which Region is the Most Profitable?
- Which City has the Highest Number of Sales?

![Report](https://user-images.githubusercontent.com/47898196/201500617-6ebd881b-1d06-495e-be61-399c482b3212.png)
