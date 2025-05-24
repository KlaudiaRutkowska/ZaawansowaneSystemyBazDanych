import os
import shutil
import logging
import csv
import oracledb
from datetime import datetime

# Dane logowania
USER         = "inf2ns_rutkowskak"
PASSWORD     = "klaudia"
HOST         = "213.184.8.44"
PORT         = 1521
SERVICE_NAME = "orcl"

# scieżki
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
SRC_DIR     = os.path.join(BASE_DIR, "incoming")
ARCHIVE_DIR = os.path.join(BASE_DIR, "archive")
LOG_DIR     = os.path.join(BASE_DIR, "logs")
LOG_FILE    = os.path.join(LOG_DIR, "loader.log")

# utworzenie katalogow, jesli ich brakuje
for d in (SRC_DIR, ARCHIVE_DIR, LOG_DIR):
    os.makedirs(d, exist_ok=True)

# Konfiguracja klienta i logowania
oracledb.init_oracle_client(lib_dir=r"C:\Users\klaudia\Desktop\instantclient_23_8")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

# walidacje
def validate_email(email):
    import re
    return bool(re.match(r'^[^@]+@[^@]+\.[^@]+$', email))

# funkcje przetwarzające CSV

def process_konferencje(path, cur):
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            try:
                cur.execute(
                    """
                    INSERT INTO konferencje
                      (nazwa, data_rozpoczecia, miejsce, opis)
                    VALUES
                      (:n, TO_DATE(:d,'YYYY-MM-DD'), :m, :o)
                    """,
                    n=r["nazwa"],
                    d=r["data_rozpoczecia"],
                    m=r.get("miejsce"),
                    o=r.get("opis")
                )
            except Exception as e:
                logging.error(f"[konferencje] {e} — wiersz: {r}")
    cur.connection.commit()

def process_sale(path, cur):
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            try:
                cur.execute(
                    """
                    INSERT INTO sale
                      (nazwa, lokalizacja, pojemnosc)
                    VALUES
                      (:n, :l, :p)
                    """,
                    n=r["nazwa"],
                    l=r["lokalizacja"],
                    p=int(r["pojemnosc"])
                )
            except Exception as e:
                logging.error(f"[sale] {e} — wiersz: {r}")
    cur.connection.commit()

def process_sesje(path, cur):
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            try:
                cur.execute(
                    """
                    INSERT INTO sesje
                      (konferencja_id, tytul, opis, data_sesji, sala_id)
                    VALUES
                      (:k, :t, :o, TO_TIMESTAMP(:ds,'YYYY-MM-DD HH24:MI:SS'), :s)
                    """,
                    k=int(r["konferencja_id"]),
                    t=r["tytul"],
                    o=r.get("opis"),
                    ds=r["data_sesji"],
                    s=int(r["sala_id"])
                )
            except Exception as e:
                logging.error(f"[sesje] {e} — wiersz: {r}")
    cur.connection.commit()

def process_prelegenci(path, cur):
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            try:
                cur.execute(
                    """
                    INSERT INTO prelegenci
                      (imie, nazwisko, biografia)
                    VALUES
                      (:i, :n, :b)
                    """,
                    i=r["imie"],
                    n=r["nazwisko"],
                    b=r.get("biografia")
                )
            except Exception as e:
                logging.error(f"[prelegenci] {e} — wiersz: {r}")
    cur.connection.commit()

def process_uczestnicy(path, cur):
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            try:
                if not validate_email(r["email"]):
                    raise ValueError(f"Invalid email: {r['email']}")
                cur.execute(
                    """
                    INSERT INTO uczestnicy
                      (imie, nazwisko, email, pesel)
                    VALUES
                      (:i, :n, :e, :p)
                    """,
                    i=r["imie"],
                    n=r["nazwisko"],
                    e=r["email"],
                    p=r.get("pesel")
                )
            except Exception as e:
                logging.error(f"[uczestnicy] {e} — wiersz: {r}")
    cur.connection.commit()

def process_rejestracje(path, cur):
    # Pobieranie list istniejacych kluczy
    cur.execute("SELECT id_konferencji FROM konferencje")
    valid_konf = {row[0] for row in cur}
    cur.execute("SELECT id_uczestnika FROM uczestnicy")
    valid_ucz = {row[0] for row in cur}

    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            k = int(r["konferencja_id"])
            u = int(r["uczestnik_id"])
            if k not in valid_konf or u not in valid_ucz:
                logging.error(f"[rejestracje] Pominięto (brak PK): konf={k},ucz={u}")
                continue
            try:
                cur.execute(
                    """
                    INSERT INTO rejestracje
                      (konferencja_id, uczestnik_id, data_rejestracji)
                    VALUES
                      (:k, :u, TO_DATE(:d,'YYYY-MM-DD'))
                    """,
                    k=k, u=u, d=r["data_rejestracji"]
                )
            except Exception as e:
                logging.error(f"[rejestracje] {e} — wiersz: {r}")
    cur.connection.commit()

def process_sesje_prelegenci(path, cur):
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            try:
                cur.execute(
                    """
                    INSERT INTO sesje_prelegenci
                      (sesja_id, prelegent_id)
                    VALUES
                      (:s, :p)
                    """,
                    s=int(r["sesja_id"]),
                    p=int(r["prelegent_id"])
                )
            except Exception as e:
                logging.error(f"[sesje_prelegenci] {e} — wiersz: {r}")
    cur.connection.commit()

# Kolejnosc przetwarzania
PROCESS_ORDER = [
    ("konferencje.csv", process_konferencje),
    ("sale.csv",        process_sale),
    ("sesje.csv",       process_sesje),
    ("prelegenci.csv",  process_prelegenci),
    ("uczestnicy.csv",  process_uczestnicy),
    ("rejestracje.csv", process_rejestracje),
    ("sesje_prelegenci.csv", process_sesje_prelegenci),
]

def main():
    dsn  = oracledb.makedsn(HOST, PORT, service_name=SERVICE_NAME)
    conn = oracledb.connect(user=USER, password=PASSWORD, dsn=dsn)
    cur  = conn.cursor()

    for fname, func in PROCESS_ORDER:
        path = os.path.join(SRC_DIR, fname)
        if os.path.exists(path):
            try:
                func(path, cur)
                ts   = datetime.now().strftime("%Y%m%d%H%M%S")
                dest = os.path.join(ARCHIVE_DIR, f"{fname}.{ts}")
                shutil.move(path, dest)
                logging.info(f"ZAŁADOWANO: {fname} → {dest}")
            except Exception:
                logging.exception(f"BLĄD przy {fname}")
        else:
            logging.info(f"Brak pliku: {fname}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
