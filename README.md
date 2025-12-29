# 🛒 E-Commerce ETL Pipeline

A complete ETL (Extract, Transform, Load) pipeline for Brazilian e-commerce data, implementing a 3-layer data architecture.

## 📊 Architecture

```
CSV Files → Staging Layer → Presentation Layer → Data Mart
```

| Layer | Purpose |
|-------|---------|
| **Staging** | Raw data ingestion from CSV files |
| **Presentation** | Cleaned, transformed, and denormalized data |
| **Data Mart** | Aggregated business metrics for analytics |

## 🗃️ Data Source

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/olistbr/brazilian-ecommerce) - Contains 100k+ orders from 2016-2018.

## 🔧 Tech Stack

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=flat-square&logo=postgresql&logoColor=white)

## 📁 Project Structure

```
csv_etl_to_datamart/
├── etl.py              # Main ETL script
├── data/               # CSV source files
│   ├── customers_dataset.csv
│   ├── orders_dataset.csv
│   ├── products_dataset.csv
│   └── ...
└── README.md
```

## 🚀 Usage

1. Configure PostgreSQL connection in `etl.py`
2. Place CSV files in `data/` directory
3. Run the pipeline:

```bash
python etl.py
```

## 📈 Output Tables

### Data Marts
- `total_sales_mart` - Overall sales metrics
- `product_categories_mart` - Sales by product category
- `products_mart` - Sales by individual product
- `sellers_mart` - Sales by seller

## 📝 License

MIT