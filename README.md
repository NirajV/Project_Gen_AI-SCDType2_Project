# Project_Gen_AI-SCDType2_Project

## Overview
This project demonstrates the implementation of **Slowly Changing Dimensions (SCD) Type 2**.
SCD Type 2 is a data warehousing technique used to track historical data by creating multiple records for a given natural key in the dimensional tables, preserving the history of changes.

## Database Setup
The database schema is defined in `setup_database.sql`. This script creates:
- `Stg_Customer`: A staging table for incoming data.
- `Dim_Customer`: A dimension table designed for SCD Type 2 tracking with `StartDate`, `EndDate`, and `IsCurrent` flags.

Reference: setup_database.sql
