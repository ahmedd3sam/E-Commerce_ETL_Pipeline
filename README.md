# E-Commerce ETL Pipeline

This project is an ETL (Extract, Transform, Load) pipeline designed to process and consolidate e-commerce data from multiple sources. It uses **PySpark** to handle data transformations and joins, merging information on customers, orders, products, and logistics. The final, cleaned dataset is intended for analysis and visualization, which can be done using the provided **Power BI** dashboard file.

The project's goals, process, and findings are further detailed in the included documentation and presentation files.

## 🛠️ Technologies Used

  * **Python**
  * **PySpark**: For large-scale data processing and transformation.
  * **Power BI**: For data visualization and dashboarding.

## 📂 Project Structure

```
.
├── Dashboard.pbix                  # Power BI Dashboard file
├── E-commerece presentation.pdf    # Project presentation
├── ETL Documentaion.docx           # Detailed ETL documentation
├── README.md                       # This README file
├── sparkscript.py                  # The main PySpark ETL script
|
├── Data (Input)
│   ├── customers.csv               # Customer dimension data
│   ├── logistics.csv               # Logistics and shipping data
│   ├── orders.csv                  # Main orders fact table
│   ├── products.csv                # Product dimension data
│   └── staging_orderlines.csv      # Order line-item details
|
└── Data (Output)
    └── fact_orders_logistics.csv   # The final, processed dataset
```

## ⚙️ ETL Workflow

The pipeline is executed by the `sparkscript.py` file and follows these steps:

1.  **Extract**: The script initializes a SparkSession and reads the following raw data files from CSVs into Spark DataFrames:

      * `orders.csv`
      * `logistics.csv`
      * `staging_orderlines.csv`
      * `products.csv`
      * `customers.csv`

2.  **Transform**: A series of joins are performed to denormalize and combine the data into a single, comprehensive table:

      * `orders` is joined with `logistics` on `order_id`.
      * `staging_orderlines` is joined with `products` on `product_id`.
      * The result of the first join (`orders_logistics`) is joined with the result of the second join (`orderlines_products`) on `order_id`.
      * The result of the previous join is joined with `customers` on `customer_id`.
      * Redundant key columns (`order_id`, `product_id`, `customer_id`) are dropped to clean the final schema.

3.  **Load**: The final transformed DataFrame is saved as a single CSV file named `fact_orders_logistics.csv`.

## 📊 Visualization

The generated `fact_orders_logistics.csv` file serves as the data source for the `Dashboard.pbix` Power BI file. The dashboard provides visual insights into key business metrics, including sales performance, customer demographics, and logistics efficiency.

## 🚀 How to Run

1.  **Install Dependencies**:

      * Ensure you have `pyspark` installed:
        ```bash
        pip install pyspark
        ```
      * You will also need a Java Development Kit (JDK) installed for Spark to run locally.

2.  **Place Data**:

      * Make sure all the source CSV files (`customers.csv`, `logistics.csv`, `orders.csv`, `products.csv`, `staging_orderlines.csv`) are in the same directory as the `sparkscript.py` script.

3.  **Run the ETL Script**:

      * Execute the PySpark script from your terminal:
        ```bash
        python sparkscript.py
        ```
      * This will run the Spark job and generate the `fact_orders_logistics.csv` output file in the same directory.

4.  **View Dashboard**:

      * Open `Dashboard.pbix` using Power BI Desktop.
      * You may need to refresh the data source and point it to the location of the newly generated `fact_orders_logistics.csv` file on your local machine.
