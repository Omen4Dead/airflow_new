import psycopg2
import csv


conn = psycopg2.connect(
    host="localhost",
    database="lastfm",
    user="postgres",
    password="postgres"
)

with open('../files/tmp/songs_20240316.csv', newline='', encoding='UTF-8') as f:
    file = csv.DictReader(f)
    headers = file.fieldnames
    for line in file:
        print(line)
