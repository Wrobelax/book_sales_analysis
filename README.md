# Book Sales Analysis

___

## **Project description**
This project consists of loading, cleaning, transforming and analyzing and visualising of data via created pipeline.
___

## *Project Structure*
```
json_rdbms/
|
├── data/                        # Data used in the project
|   ├── DATA1
|   ├── DATA2
|   └── DATA3
├── src/
|   ├── analysis.py              # Script used for data analysis
|   └── etl.py                   # Script used for data loading and cleaning
├── output/
|   ├── clean.json               # Fixed JSON file
|   └── database.db              # Database generated of JSON file
├── requirements.txt
├── dashboard.py                 # Script used for data visualization
└── pipeline.py                  # Main file used for orchestration of the pipeline.
```
___

## *Data used*
1. books.yaml
2. users.csv
3. orders.parquet

___

## *Pipeline runs*
1. Cleaning and normalization of data
2. Duplicates and bad rows removal
3. Data type conversion
4. Date transformation
5. Authors cleaning
6. Revenue formatting

___

## *Installation*

### 1.Clone the repository:
```bash
git clone https://github.com/Wrobelax/book_sales_analysis.git
cd book_sales_analysis
```

### 2.Install requirements:
```bash
pip install -r requirements.txt
```

### 3. Run pipeline:
```bash
python pipeline.py
```

### 4. Run the dashboard:
```bash
streamlit run dashboard.py

```
