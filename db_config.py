import psycopg2

# connect to db
conection = psycopg2.connect(
    host='127.0.0.1',
    user='postgres',
    password='admin',
    database='test')
conection.autocommit = True
con = conection
