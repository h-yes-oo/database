from mysql.connector import connect

connection = connect(
  host='astronaut.snu.ac.kr',
  port=7000,
  user='DB2015_16227',
  password='DB2015_16227',
  db='DB2015_16227',
  charset='utf8'
)

with connection.cursor(dictionary=True) as cursor:
  cursor.execute("show tables;")
  result = cursor.fetchall()
  print(result)

connection.close()