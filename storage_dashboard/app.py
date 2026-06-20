import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import fsspec

st.title("🚏 ShelterEye - Transjakarta Real-Time Analytics Dashboard")
st.subheader("📊 Tren Kepadatan Penumpang Live (Hadoop HDFS Source)")

hdfs_path = "hdfs://localhost:9000/lakehouse_data/gold/eta_halte"

try:
    # Mengakses file system Hadoop langsung dari Python
    fs = fsspec.filesystem("hdfs", host="localhost", port=9000)
    
    if fs.exists("/lakehouse_data/gold/eta_halte"):
        dataset = pq.ParquetDataset(hdfs_path, filesystem=fs)
        table = dataset.read()
        df = table.to_pandas()
        
        # Tampilkan grafik batang live
        st.bar_chart(df.set_index("halte_name")["total_penumpang"])
        st.dataframe(df)
    else:
        st.info("Menunggu data matang dikirim oleh Spark ke Gold Layer HDFS...")
except Exception as e:
    st.error(f"Gagal terhubung ke cluster HDFS: {e}")