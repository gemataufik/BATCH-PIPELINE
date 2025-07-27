+ Buat file docker-compose.yml dan Dockerfile 
   - Setup Airflow + PostgreSQL pake Docker Compose

+ Buat .env untuk menyimpan konfigurasi sensitif dan koneksi
   - Salin file `.env.(contoh)` menjadi `.env`
   - Generate FERNET_KEY:
      - python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   - Setup alert menggunakan telegram, masukan TELEGRAM_CHAT_ID dan TELEGRAM_CHAT_ID di .env

+ Buat data_gen/generate.py dan dags/insert_data_dag.py 
   - Untuk Generate data dummy ke Postgres otomatis tiap 1 jam
   note : untuk DAG ini tidak menggunakan alert kerena pada real case biasanya database sudah tersedia

+ Buat dags/ingest_postgres_to_bq_dag.py
   - Membuat fungsi notifikasi error ke telgram
   - extrac dari postgres data h-1 berdasarkan created_at  
   - menyimpannya ke GCS sebagai csv (tempat perantara)
   - Meload data dari GCS ke BigQuery ke dalam tabel staging , dengan partisi berdasarkan created_at

+ Buat dags/dim_fact_mart_dag.py
   - Mengisi table dim dan fact dari table staging
   - membuat metriks datamart dari tabel dim dan fact 


======== UNTUK MENJALANKAN PROGRAM INI ===========
+ docker compose up --build
+ setelah running untuk "insert_data_dag.py" edit dulu dari catchup=False jadi catchup=True
   - atau bisa backfill manual lewat container -> airflow dags backfill nama_dag -s 2025-07-21 -e 2025-07-23
   - jika sudah terisi datanya sampai hari sekarang boleh di ganti lagi ke catchup=False

+ jika insert_data_toko_dag sudah di jalankan bisa cek di DBever terlebih dahulu

+ untuk connect ke DBever/lokal bisa gunakan POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB dan ports dari project_postgres yang ada di docker-compose.yml

+ jika sudah masuk boleh jalankan ingest_postgres_to_bq_dag terlebih dahulu baru dim_fact_datamart_dag

+ Jangan lupa tambahkan key.json di folder keys dan docker-compose.yml
   









   
