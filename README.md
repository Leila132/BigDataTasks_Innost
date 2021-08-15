# BigDataTasks_Innost
Using this task, you can read data from a file with the extension .pcap, parse them and upload to Spark. 

Instead of file table.pcap, you can insert your own file with the specified extension. Since I used the Wireshark application to get a table with Ethernet traffic, I could not get data about ports in IPv6 connections. Therefore, in these connections, the ports are assigned the value "55555".

Next, the table is loaded into a parquet file with the extension .gzip, and then are grouped in various ways.
