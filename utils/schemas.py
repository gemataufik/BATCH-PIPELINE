from google.cloud import bigquery

mart_schemas = {
    "total_penjualan_per_hari_per_toko": [
        bigquery.SchemaField("store_id", "STRING"),
        bigquery.SchemaField("store_name", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("total_penjualan", "FLOAT"),
    ],
    "jumlah_transaksi_per_toko": [
        bigquery.SchemaField("store_id", "STRING"),
        bigquery.SchemaField("store_name", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("jumlah_transaksi", "INTEGER"),
    ],
    "rata_rata_pembelian_per_customers": [
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("customer_name", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("avg_pembelian", "FLOAT"),
    ],
    "product_paling_laku_per_toko": [
        bigquery.SchemaField("store_id", "STRING"),
        bigquery.SchemaField("product_name", "STRING"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("total_terjual", "INTEGER"),
        bigquery.SchemaField("date", "DATE"),
    ],
    "avg_transaction": [
        bigquery.SchemaField("store_id", "STRING"),
        bigquery.SchemaField("store_name", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("avg_transaksi", "FLOAT"),
    ],
}
