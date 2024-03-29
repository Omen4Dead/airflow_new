import psycopg2


conn = psycopg2.connect(
    host="localhost",
    database="lastfm",
    user="postgres",
    password="postgres"
)

print(conn)