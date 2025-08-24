# CSV Data Processing Project

This project consists of two main parts:
1. A Python script to generate a large CSV file with random sales data.
2. A TypeScript program to process this CSV file in batches, group data by year and month, and calculate various sales statistics.

## Table of Contents
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Testing](#testing)

## Features

### Python CSV Generator (`generate_csv.py`)
- Generates a `sales_data.csv` file with 2,000,000 random records.
- Each record includes the following fields:
    - `ID` (Integer)
    - `order_id` (Integer)
    - `customer_id` (Integer)
    - `total` (Float)
    - `fecha` (Datetime)

### TypeScript Data Processor (`src/process_sales.ts`)
- Reads the large `sales_data.csv` file using a streaming, batch-processing approach to handle memory constraints.
- Groups sales data by year and month.
- Calculates the following statistics for each year and month:
    - Number of sales
    - Maximum `total` value
    - Minimum `total` value
    - Average `total` value
    - Standard deviation of `total` values
- Outputs the aggregated results to `yearly_monthly_sales.csv`.

## Installation

To set up and run this project, follow these steps:

### 1. Clone the repository (if applicable)

```bash
git clone <repository_url>
cd curso-ejercicio999-cursor
```

### 2. Install Python dependencies

No external Python libraries are required beyond standard Python 3.x.

### 3. Install Node.js dependencies

Make sure you have Node.js and npm (or yarn) installed.

```bash
npm install
```

## Usage

### 1. Generate the CSV data file

First, run the Python script to create the `sales_data.csv` file. This might take a few minutes due to the large number of records.

```bash
python generate_csv.py
```

This will create a `sales_data.csv` file in the project root directory.

### 2. Process the sales data

Next, run the TypeScript program to process the generated CSV file and aggregate the data.

```bash
npm start
```

This will generate a `yearly_monthly_sales.csv` file in the project root directory, containing the aggregated sales statistics.

## Project Structure

```
curso-ejercicio999-cursor/
├── generate_csv.py                 # Python script to generate random CSV data
├── src/
│   └── process_sales.ts            # TypeScript script for data processing
├── test/
│   └── process_sales.test.ts       # Unit tests for the TypeScript data processor
├── package.json                    # Node.js project configuration and dependencies
├── tsconfig.json                   # TypeScript compiler configuration
└── README.md                       # This file
```

## Testing

To run the unit tests for the TypeScript data processing script:

```bash
npm test
```

This will compile the TypeScript test files and execute them using Mocha and Chai.
# ej999-PF-A-cursor
