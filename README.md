# GDP Data ETL (Extract, Transform, Load) Project Using Python

## Introduction

This project demonstrates an ETL process using Python, focusing on global GDP data. It entails extracting GDP data from a web source, transforming it for analytical readiness, and loading it into a MySQL database for further analysis.

## Project Overview

1. **Data Extraction:**
   - Extract global GDP data in HTML format from a web archive link using web scraping techniques.
   
2. **Data Transformation:**
   - Convert GDP values from millions to billions using Python's Pandas and NumPy libraries.
   - Rename dataframe columns for clarity and consistency.

3. **Data Load:**
   - Load the transformed data into a MySQL database.
   - Perform SQL operations to create and populate database tables.

4. **Query and Log:**
   - Execute SQL queries to interact with the database.
   - Log each step of the process for debugging and tracking.

## Technical Aspects

- Utilizes Python libraries such as `requests`, `pandas`, `BeautifulSoup`, `numpy`, and `mysql.connector`.
- Showcases data handling capabilities with web scraping, data manipulation, and database interaction.
- Features error handling and logging for robust process execution.

## Execution Guide

- **Setting Up:**
  - Ensure all required Python libraries are installed.
  - Set up the MySQL database and configure connection parameters.

- **Running the Code:**
  - The script is structured into modular functions, each responsible for a phase of the ETL process.
  - Execute the script to perform the entire ETL process, or run individual functions as needed.

- **Logging:**
  - Progress and errors are logged to `log_file.text`, facilitating process monitoring and troubleshooting.

## Requirements

- Python (preferably version 3.11.3 or later)
- MySQL (preferably version 8.0.34 or later)
- Python libraries: `pandas`, `requests`, `BeautifulSoup`, `numpy`, `mysql.connector`

## Installation

Install the necessary Python libraries using pip:

```sh
pip install requests pandas numpy beautifulsoup4 mysql-connector-python
```

## License

The source code is available under the MIT license. See LICENSE for more information.

## Acknowledgments

This project was inspired by various resources and similar projects in the field of data science. Special thanks to all contributors and the open-source community.

© Copyright 2023 João Henrique. All rights reserved.
