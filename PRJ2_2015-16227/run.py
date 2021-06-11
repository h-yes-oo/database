from __future__ import print_function
import mysql.connector
from mysql.connector import connect
from mysql.connector import errorcode

connection = connect(
  host='astronaut.snu.ac.kr',
  port=7000,
  user='DB2015_16227',
  password='DB2015_16227',
  db='DB2015_16227',
  charset='utf8'
)
#make table schemas

TABLES = {}
TABLES['building'] = (
    "CREATE TABLE `building` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `name` varchar(200) NOT NULL,"
    "  `location` varchar(200) NOT NULL,"
    "  `capacity` int(11) NOT NULL,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB")

TABLES['performance'] = (
    "CREATE TABLE `performance` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `name` varchar(200) NOT NULL,"
    "  `type` varchar(200) NOT NULL,"
    "  `price` int(11) NOT NULL,"
    "  `building` int(11),"
    "  PRIMARY KEY (`id`),"
    "  CONSTRAINT `performance_ibfk_1` FOREIGN KEY (`building`) "
    "     REFERENCES `building` (`id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

TABLES['audience'] = (
    "CREATE TABLE `audience` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `name` varchar(200) NOT NULL,"
    "  `gender` char(1) NOT NULL,"
    "  `age` int(11) NOT NULL,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB")

TABLES['reservation'] = (
    "  CREATE TABLE `reservation` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `audience` int(11) NOT NULL,"
    "  `performance` int(11) NOT NULL,"
    "  `seat` int(11) NOT NULL,"
    "  PRIMARY KEY (`id`),"
    "  CONSTRAINT `reservation_ibfk_1` FOREIGN KEY (`audience`) "
    "     REFERENCES `audience` (`id`) ON DELETE CASCADE,"
    "  CONSTRAINT `reservation_ibfk_2` FOREIGN KEY (`performance`) "
    "     REFERENCES `performance` (`id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

def create_tables(cursor, tables):
  for table_name in tables:
    table_description = tables[table_name]
    try:
      cursor.execute(table_description)
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
        print("already exists.")
      else:
        print(err.msg)


def insert_building(cursor, name, location, capacity):
  add_building = ("INSERT INTO building "
               "(name, location, capacity) "
               "VALUES (%s, %s, %s)" )
  data_building = (name, location, capacity)
  cursor.execute(add_building, data_building)
  connection.commit()

def insert_performance(cursor, name, ptype, price):
  add_performance = ("INSERT INTO performance "
               "(name, type, price) "
               "VALUES (%s, %s, %s)" )
  data_performance = (name, ptype, price)
  cursor.execute(add_performance, data_performance)
  connection.commit()

def insert_audience(cursor, name, gender, age):
  add_audience = ("INSERT INTO audience "
               "(name, gender, age) "
               "VALUES (%s, %s, %s)" )
  data_audience = (name, gender, age)
  cursor.execute(add_audience, data_audience)
  connection.commit()

def check_building_assigned(cursor, pid):
  #check if the performance is already assigned to building
  check = (
    "SELECT building FROM performance WHERE id = %s"
  )
  cursor.execute(check, (pid,))
  for data in cursor:
    #if assigned, return false
    if data['building'] is None:
      return False
    else:
      return True

def assign_p_to_b(cursor, pid, bid):
  #check if the performance is already assigned to building
  check = check_building_assigned(cursor,pid)
  if(check):
    print("Performance %d is already assigned" % (pid))
    return
  #if function not returned, assign performance to building
  update = (
    "UPDATE performance SET building = %s "
    "WHERE id = %s"
  )
  cursor.execute(update, (bid, pid))
  connection.commit()

def check_seat(cursor, pid, seat):
  check = (
    "SELECT 1 FROM reservation WHERE performance = %s AND seat = %s"
  )
  cursor.execute(check,(pid, seat))
  #if there exists data in cursor, it means the seat of the performance is already taken
  for data in cursor:
    print("The seat is already taken")
    return False
  return True

def reserve(cursor, pid, aid, seat):
  add_performance = ("INSERT INTO reservation "
               "(audience, performance, seat) "
               "VALUES (%s, %s, %s)" )
  data_performance = (aid, pid, seat)
  cursor.execute(add_performance, data_performance)
  connection.commit()

def print_line():
  print("-----------------------------------------------------------------------------------")

def print_building(curA, curB):
  print_line()
  print("%-8s %-30s %-16s %-16s %-16s" %('id','name','location','capacity','assigned'))
  print_line()
  curA.execute("SELECT * FROM building ORDER BY id")
  result = curA.fetchall()
  for data in result:
    count_query = ("SELECT COUNT(id) FROM performance WHERE building = %s")
    curB.execute(count_query, (data['id'], ))
    for x in curB:
      print("%-8s %-30s %-16s %-16s %-16s" %(
        data['id'], data['name'], data['location'], data['capacity'], x[0]
      ))
  print_line()

def print_audience(result):
  print_line()
  print("%-8s %-36s %-22s %-20s" %('id','name','gender','age'))
  print_line()
  for data in result:
    print("%-8s %-36s %-22s %-20s" %(
      data['id'], data['name'], data['gender'], data['age']
    ))
  print_line()

def print_audience_all(cursor):
  cursor.execute("SELECT * FROM audience ORDER BY id")
  result = cursor.fetchall()
  print_audience(result)

def print_audience_of_performance(cursor, pid):
  select_query = (
    "SELECT * FROM audience "
    "WHERE id IN ("
    "SELECT DISTINCT audience FROM reservation "
    "WHERE performance = %s) "
    "ORDER BY id"
  )
  cursor.execute(select_query, (pid,))
  result = cursor.fetchall()
  print_audience(result)

def print_performance(curB, result):
  print_line()
  print("%-8s %-30s %-16s %-16s %-16s" %('id','name','type','price','booked'))
  print_line()
  for data in result:
    count_query = ("SELECT COUNT(id) FROM reservation WHERE performance = %s")
    curB.execute(count_query, (data['id'], ))
    for x in curB:
      print("%-8s %-30s %-16s %-16s %-16s" %(
        data['id'], data['name'], data['type'], data['price'], x[0]
      ))
  print_line()

def print_performance_all(curA, curB):
  curA.execute("SELECT * FROM performance ORDER BY id")
  result = curA.fetchall()
  print_performance(curB, result)

def print_performance_of_building(curA, curB, bid):
  select_query = (
    "SELECT * FROM performance "
    "WHERE building = %s ORDER BY id"
  )
  curA.execute(select_query, (bid,))
  result = curA.fetchall()
  print_performance(curB, result)

def refresh(cursor, tables):
  for table_name in ['reservation', 'audience', 'performance','building']:
    sql = "DROP TABLE IF EXISTS %s" % table_name
    cursor.execute(sql)
  create_tables(cursor,tables)

def delete(cursor, table, did):
  delete_query = (
    "DELETE FROM {} WHERE id = %s".format(table)
  )
  cursor.execute(delete_query,(did,))


curA = connection.cursor(dictionary=True, buffered=True)
curB = connection.cursor(dictionary=False, buffered=True)

try:
    refresh(curA, TABLES)
    insert_building(curA, 'b1', 'l1', 10)
    insert_building(curA, 'b2', 'l2', 20)
    insert_performance(curA, 'chicago','musical', 100000)
    insert_performance(curA, 'haha', 'drama', 1400)
    insert_audience(curA, 'hyesoo', 'F', 26)
    insert_audience(curA, 'sangwoo', 'M', 26)
    assign_p_to_b(curA, 1, 2)
    assign_p_to_b(curA, 2, 1)
    reserve(curA,1, 1, 1)
    reserve(curA,1, 1, 2)
    reserve(curA,1, 2, 3)
    check_seat(curA, 1, 1)
    print_building(curA, curB)
    print_performance_all(curA, curB)
    print_audience(curA)
    print_performance_of_building(curA, curB, 2)
    print_audience_of_performance(curA, 1)

    curA.execute("SELECT * FROM reservation")
    result = curA.fetchall()
    print(result)

except mysql.connector.Error as err:
    print(err)
    exit(1)

curA.close()
curB.close()
connection.close()