# test_connect.py
from config import DB_CONFIG
import oracledb

# ustawienie Oracle Instant Client (thick mode)
oracledb.init_oracle_client(lib_dir=r"C:\Users\klaudia\Desktop\instantclient_23_8")

# dane logowania
user         = "inf2ns_rutkowskak"
password     = "klaudia"
host         = "213.184.8.44"
port         = 1521
service_name = "orcl"

# budowa DSN i połączenie
dsn = oracledb.makedsn(host, port, service_name=service_name)
conn = oracledb.connect(user=user, password=password, dsn=dsn)

print("Połączono z Oracle! Wersja serwera:", conn.version)

conn.close()
