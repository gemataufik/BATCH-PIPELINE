PROJECT_ID = "purwadika"
DATASET = "jdeol003_finpro_gema"

mart_queries = {
    "total_penjualan_per_hari_per_toko": f"""
        SELECT
            ft.store_id,
            ds.store_name,
            DATE(ft.created_at) AS date,
            SUM(ft.total_amount) AS total_penjualan
        FROM `{PROJECT_ID}.{DATASET}.fact_transactions` ft
        LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_stores` ds 
            ON ft.store_id = ds.store_id
        WHERE DATE(ft.created_at) = '{{{{ macros.ds_add(ds, -1) }}}}'
        GROUP BY ft.store_id, ds.store_name, date
    """,

    "jumlah_transaksi_per_toko": f"""
        SELECT
            ft.store_id,
            ds.store_name,
            DATE(ft.created_at) AS date,
            COUNT(*) AS jumlah_transaksi
        FROM `{PROJECT_ID}.{DATASET}.fact_transactions` ft
        LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_stores` ds ON ft.store_id = ds.store_id
        WHERE DATE(ft.created_at) = '{{{{ macros.ds_add(ds, -1) }}}}'
        GROUP BY ft.store_id, ds.store_name, date
    """,

    "rata_rata_pembelian_per_customers": f"""
        SELECT
            ft.customer_id,
            dc.customer_name,
            DATE(ft.created_at) AS date,
            AVG(ft.total_amount) AS avg_pembelian
        FROM `{PROJECT_ID}.{DATASET}.fact_transactions` ft
        LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_customers` dc ON ft.customer_id = dc.customer_id
        WHERE DATE(ft.created_at) = '{{{{ macros.ds_add(ds, -1) }}}}'
        GROUP BY ft.customer_id, dc.customer_name, date
    """,

    "product_paling_laku_per_toko": f"""
        SELECT
            final.store_id,
            dp.product_name,
            final.product_id,
            final.total_terjual,
            final.tx_date AS date
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY store_id, tx_date
                    ORDER BY total_terjual DESC
                ) AS rank
            FROM (
                SELECT 
                    ft.store_id,
                    ft.product_id,
                    DATE(ft.created_at) AS tx_date,
                    SUM(ft.quantity) AS total_terjual
                FROM `{PROJECT_ID}.{DATASET}.fact_transactions` ft
                WHERE DATE(ft.created_at) = '{{{{ macros.ds_add(ds, -1) }}}}'
                GROUP BY ft.store_id, ft.product_id, tx_date
            )
        ) AS final
        LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_products` dp 
            ON final.product_id = dp.product_id
        WHERE rank = 1
    """,

    "avg_transaction": f"""
        SELECT 
            ft.store_id,
            ds.store_name,
            DATE(ft.created_at) AS date,
            AVG(ft.total_amount) AS avg_transaksi
        FROM `{PROJECT_ID}.{DATASET}.fact_transactions` ft
        LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_stores` ds ON ft.store_id = ds.store_id
        WHERE DATE(ft.created_at) = '{{{{ macros.ds_add(ds, -1) }}}}'
        GROUP BY ft.store_id, ds.store_name, date
    """
}
