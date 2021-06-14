from __future__ import print_function
import mysql.connector
from mysql.connector import errorcode

#create table with tables schema
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

#insert building, performance, audience function with their schema
def insert_building(cursor, name, location, capacity):
  add_building = ("INSERT INTO building "
               "(name, location, capacity) "
               "VALUES (%s, %s, %s)" )
  data_building = (name, location, capacity)
  cursor.execute(add_building, data_building)

def insert_performance(cursor, name, ptype, price):
  add_performance = ("INSERT INTO performance "
               "(name, type, price) "
               "VALUES (%s, %s, %s)" )
  data_performance = (name, ptype, price)
  cursor.execute(add_performance, data_performance)

def insert_audience(cursor, name, gender, age):
  add_audience = ("INSERT INTO audience "
               "(name, gender, age) "
               "VALUES (%s, %s, %s)" )
  data_audience = (name, gender, age)
  cursor.execute(add_audience, data_audience)

#return if the performance is already assigned to building
def check_building_assigned(cursor, pid):
  check = (
    "SELECT building FROM performance WHERE id = %s"
  )
  cursor.execute(check, (pid,))
  for data in cursor:
    #if not assigned, return false
    if data['building'] is None:
      return False
    else:
      return True

#return if assigning performance to building succeed
def assign_p_to_b(cursor, pid, bid):
  #check if the performance is already assigned to building
  check = check_building_assigned(cursor,pid)
  if(check):
    print("Performance %d is already assigned" % (pid))
    return False
  #if function not returned, assign performance to building
  update = (
    "UPDATE performance SET building = %s "
    "WHERE id = %s"
  )
  cursor.execute(update, (bid, pid))
  return True

#return if id exists in table
def check_by_id(cursor, table, bid):
  check = (
    "SELECT 1 FROM {} WHERE id = %s".format(table)
  )
  cursor.execute(check,(bid, ))
  result = cursor.fetchall()
  if len(result) > 0:
    return True
  else:
    return False

#return if the seat of pid already taken
def seat_taken(cursor, pid, seat):
  check = (
    "SELECT 1 FROM reservation WHERE performance = %s AND seat = %s"
  )
  cursor.execute(check,(pid, seat))
  result = cursor.fetchall()
  if len(result) > 0:
    print("The seat is already taken")
    return True
  else:
    return False

#insert new data to reservation table
def reserve(cursor, pid, aid, seat):
  add_performance = ("INSERT INTO reservation "
               "(audience, performance, seat) "
               "VALUES (%s, %s, %s)" )
  data_performance = (aid, pid, seat)
  cursor.execute(add_performance, data_performance)

#print a line
def print_line():
  print("-----------------------------------------------------------------------------------")

#print all building
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

#basic audience printing format
def print_audience(result):
  print_line()
  print("%-8s %-36s %-22s %-20s" %('id','name','gender','age'))
  print_line()
  for data in result:
    print("%-8s %-36s %-22s %-20s" %(
      data['id'], data['name'], data['gender'], data['age']
    ))
  print_line()

#print all audiences
def print_audience_all(cursor):
  cursor.execute("SELECT * FROM audience ORDER BY id")
  result = cursor.fetchall()
  print_audience(result)

#print audience of specific performance
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

#basic performance printing format
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

#print all performances 
def print_performance_all(curA, curB):
  curA.execute("SELECT * FROM performance ORDER BY id")
  result = curA.fetchall()
  print_performance(curB, result)

#print performance of specific building
def print_performance_of_building(curA, curB, bid):
  select_query = (
    "SELECT * FROM performance "
    "WHERE building = %s ORDER BY id"
  )
  curA.execute(select_query, (bid,))
  result = curA.fetchall()
  print_performance(curB, result)

#return capacity of the performance
def get_capacity(curB, pid):
  select_query = (
    "SELECT capacity FROM building "
    "WHERE id = ("
    "SELECT building FROM performance "
    "WHERE id = %s)"
  )
  curB.execute(select_query, (pid, ))
  result = curB.fetchall()
  capacity = result[0][0]
  return capacity

#using for loop, print seat of performance
def print_seat_of_performance(curB, pid):
  capacity = get_capacity(curB, pid)
  print_line()
  print("%-40s %-40s" %('seat_number', 'audience_id'))
  print_line()
  for seat in range(1,capacity+1):
    find_query = (
      "SELECT audience FROM reservation "
      "WHERE performance = %s and seat = %s"
    )
    curB.execute(find_query, (pid, seat))
    result = curB.fetchall()
    if(len(result) == 0):
      print("%-40d" %(seat))
    else:
      print("%-40d %-40d" %(seat, result[0][0]))
  print_line()

#delete existing table and create new tables
def refresh(cursor, tables):
  for table_name in ['reservation', 'audience', 'performance','building']:
    sql = "DROP TABLE IF EXISTS %s" % table_name
    cursor.execute(sql)
  create_tables(cursor,tables)

#delete from table instance with did
def delete(cursor, table, did):
  delete_query = (
    "DELETE FROM {} WHERE id = %s".format(table)
  )
  cursor.execute(delete_query,(did,))

#return if all the seat in seat list can be reserved
def can_reserve(curB, seat_list, pid):
  capacity = get_capacity(curB, pid)
  for seat in seat_list:
    if seat > capacity:
      print("Seat number out of range")
      return False
  for seat in seat_list:
   if seat_taken(curB, pid, seat):
     return False
  return True

#return one ticket price with respect to the audience's age
def get_ticket_price(curB, pid, aid):
  age_query = (
    "SELECT age FROM audience "
    "WHERE id = %s"
  )
  curB.execute(age_query, (aid,))
  age_result = curB.fetchall()
  age = age_result[0][0]
  if age <= 7:
    return 0
  price_query = (
    "SELECT price FROM performance "
    "WHERE id = %s"
  )
  curB.execute(price_query, (pid,))
  price_result = curB.fetchall()
  price = price_result[0][0]
  if 8 <= age and age <= 12:
    return price * 0.5
  elif 13 <= age and age <= 18:
    return price * 0.8
  else:
    return price
