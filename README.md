# Database-Engine

This project is a simplified database engine implemented in Java, designed as part of the CSEN604: Database II course. The engine supports table creation, data insertion, deletion, updates, and querying with optional Octree-based indexing for efficient search and data retrieval.

## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Contributors](#contributors)


## Introduction
The goal of this project is to simulate the core functionalities of a database engine, including support for:
- Storing data in disk-based binary pages for optimized memory usage.
- Handling CRUD operations on tables.
- Implementing Octree indices for efficient multidimensional queries.

This project was completed in two phases:
1. Basic CRUD operations without indexing.
2. Enhanced functionality with Octree-based indexing.

## Features
- **Tables and Pages:**
  - Tables are stored as binary files on disk, with rows organized in pages.
  - Efficient memory management with lazy loading of pages.
  
- **CRUD Operations:**
  - **Create Table:** Define schema with clustering keys, data types, and value ranges.
  - **Insert Row:** Add data to tables with automatic page and memory management.
  - **Delete Rows:** Remove rows based on conditions; handles empty pages by deleting them.
  - **Update Rows:** Modify existing rows based on the clustering key.
  
- **Octree Indexing:**
  - Supports creating Octree indices for up to three columns.
  - Improves query performance by reducing search space.
  - Dynamically updates indices during insert, update, and delete operations.

- **Query Execution:**
  - Supports range queries, equality checks, and logical operators (`AND`, `OR`, `XOR`).
  - Automatically uses indices for optimized query execution when available.

## Technologies Used
- **Programming Language:** Java
- **Data Structures:** Java Vectors, Serializable Objects
- **File Handling:** Binary serialization for pages and indices


## Contributors
+ [Farah Maher](https://github.com/farahalfawzy)
+ [Seif Hossam](https://github.com/seifhossam2002)
+ [Malak El Wassif](https://github.com/malakElWassif)
+ [Paula Iskander](https://github.com/paula-iskander)
+ [Tony Iskander](https://github.com/toniskander)
