import dpkt
import socket
import pandas as pd
from pyspark.sql import SparkSession
import fastparquet
from fastparquet import write 

with open('table.pcap','rb') as pcap_in:
  contents = dpkt.pcap.Reader(pcap_in)  

  src_ip_ = []
  dst_ip_ = []
  s_port_ = []
  d_port_ = []
  bytes_cnt_ = []
  

  for ts, buf in contents:
    eth = dpkt.ethernet.Ethernet(buf)
    ip = eth.data
    tcp = ip.data
    try:
        src_ip = socket.inet_ntop(socket.AF_INET,eth.data.src)
        dst_ip = socket.inet_ntop(socket.AF_INET, eth.data.dst)
    except OSError:
        src_ip = socket.inet_ntop(socket.AF_INET6, eth.data.src)
        dst_ip = socket.inet_ntop(socket.AF_INET6, eth.data.dst) 
    except ValueError: 
        src_ip = socket.inet_ntop(socket.AF_INET6, eth.data.src)
        dst_ip = socket.inet_ntop(socket.AF_INET6, eth.data.dst)
    
    try:
        s_port = tcp.sport
        d_port = tcp.dport
    except AttributeError:
        s_port = 55555
        d_port = 55555
    bytes_cnt = len(buf)

    src_ip_.append(src_ip)
    dst_ip_.append(dst_ip)
    s_port_.append(s_port)
    d_port_.append(d_port)
    bytes_cnt_.append(bytes_cnt)

pandas_df = pd.DataFrame({
    'Sourse': src_ip_,
    'Destination': dst_ip_,
    'Source_Port': s_port_,
    'Destination_Port': d_port_,
    'Bytes': bytes_cnt_
})
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(pandas_df)
pandas_df.to_parquet('df.parquet.gzip', compression='gzip')  
"""
print(pd.read_parquet('df.parquet.gzip'))
"""
df.groupby('Sourse').count().show()
df.groupby('Sourse', 'Destination').count().show()
df.groupby('Sourse').sum('Bytes').show()
df.groupby('Sourse', 'Destination').sum('Bytes').show()

    
    





    