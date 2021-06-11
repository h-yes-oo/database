from __future__ import print_function
import mysql.connector
from mysql.connector import connect
from mysql.connector import errorcode

try:
  connection = connect(
    host='astronaut.snu.ac.kr',
    port=7000,
    user='DB2015_16227',
    password='DB2015_16227',
    db='DB2015_16227',
    charset='utf8'
  )
except:
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
    #if not assigned, return false
    if data['building'] is None:
      return False
    else:
      return True

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

curA = connection.cursor(dictionary=True, buffered=True)
curB = connection.cursor(dictionary=False, buffered=True)

info_message = '''
============================================================ 
1. print all buildings
2. print all performances
3. print all audiences
4. insert a new building
5. remove a building
6. insert a new performance
7. remove a performance
8. insert a new audience
9. remove an audience
10. assign a performance to a building
11. book a performance
12. print all performances which assigned at a building
13. print all audiences who booked for a performance
14. print ticket booking status of a performance
15. exit
16. reset database
============================================================
'''

invalid_message = "Invalid action"

print(info_message)
while(True):
  print(" ")
  try:
    action = int(input("Select your action: "))
    if action >= 17 or action <= 0:
      print(invalid_message)
    else:
      if action == 1:
        print_building(curA, curB)
      elif action == 2:
        print_performance_all(curA, curB)
      elif action == 3:
        print_audience_all(curA)
      elif action == 4:
        name = input("Building name: ")
        location = input("Building location: ")
        try:
          capacity = int(input("Building capacity: "))
          if capacity <= 0:
            print("Capacity should be more than 0")
            continue
          else:
            insert_building(curA, name, location, capacity)
            connection.commit()
            print("A building is successfully inserted")
        except:
          print("Invalid input")
          continue
      elif action == 5:
        try:
          bid = int(input("Building id: "))
          if(check_by_id(curA, 'building', bid)):
            delete(curA, 'building', bid)
            print("A building is successfully removed")
          else:
            print("Building {} doesn’t exist".format(bid))
        except:
          print("Invalid input")

      elif action == 6:
        name = input("Performance name: ")
        ptype = input("Performance type: ")
        try:
          price = int(input("Performance price: "))
          if price < 0:
            print("Price should be 0 or more")
            continue
          else:
            insert_performance(curA, name, ptype, price)
            connection.commit()
            print("A performance is successfully inserted")
        except:
          print("Invalid input")
          continue

      elif action == 7:
        try:
          pid = int(input("Performance id: "))
          if(check_by_id(curA, 'performance', pid)):
            delete(curA, 'performance', pid)
            print("A performance is successfully removed")
          else:
            print("Performance {} doesn’t exist".format(pid))
        except:
          print("Invalid input")
      
      elif action == 8:
        name = input("Audience name: ")
        gender = input("Audience type: ")
        if gender != "M" and gender != "F":
          print("Gender should be 'M' or 'F'")
          continue
        try:
          age = int(input("Audience age: "))
          if age < 0:
            print("Age should be 0 or more")
            continue
          else:
            insert_audience(curA, name, gender, age)
            connection.commit()
            print("An audience is successfully inserted")
        except:
          print("Invalid input")
          continue
      
      elif action == 9:
        try:
          aid = int(input("Audience id: "))
          if(check_by_id(curA, 'audience', aid)):
            delete(curA, 'audience', aid)
            print("An audience is successfully removed")
          else:
            print("Audience {} doesn’t exist".format(aid))
        except:
          print("Invalid input")
      
      elif action == 10:
        try:
          bid = int(input("Building ID: "))
          if not check_by_id(curA, 'building', bid):
            print("Building {} doesn’t exist".format(bid))
            continue
          pid = int(input("Performance ID: "))
          if not check_by_id(curA, 'performance', pid):
            print("Performance {} doesn’t exist".format(pid))
            continue
          if(assign_p_to_b(curA, pid, bid)):
            print("Successfully assign a performance")
            connection.commit()
        except:
          print("Invalid input")
      
      elif action == 11:
        try:
          pid = int(input("Performance ID: "))
          if not check_building_assigned(curA, pid):
            print("Performance {} isn't assigned".format(pid))
            continue
          aid = int(input("Audience ID: "))
          seat_list = list(map(int, input("Seat number: ").split(',')))
          if(can_reserve(curB, seat_list, pid)):
            for seat in seat_list:
              reserve(curA, pid, aid, seat)
            price = get_ticket_price(curB, pid, aid)
            total_price = round(price * len(seat_list))
            print("Successfully book a performance.")
            print("Total ticket price is {:,}".format(total_price))
        except:
          print("Invalid input")

      elif action == 12:
        try:
          bid = int(input("Building ID: "))
          if check_by_id(curA, 'building', bid):
            print_performance_of_building(curA, curB, bid)
          else:
            print("Audience {} doesn’t exist".format(bid))
        except:
          print("Invalid input")

      elif action == 13:
        try:
          pid = int(input("Performance ID: "))
          if check_by_id(curA, 'performance', pid):
            print_audience_of_performance(curA, pid)
          else:
            print("Performance {} doesn’t exist".format(pid))
        except:
          print("Invalid input")
      
      elif action == 14:
        try:
          pid = int(input("Performance ID: "))
          if check_by_id(curA, 'performance', pid):
            if check_building_assigned(curA, pid):
              print_seat_of_performance(curB, pid)
            else:
              print("Performance {} isn't assigned".format(pid))
          else:
            print("Performance {} doesn’t exist".format(pid))
        except:
          print("Invalid input")

      elif action == 15:
        print("Bye!")
        break

      elif action == 16:
        flag = input("Every table and data will be deleted. Go on ? (y/n) : ")
        if flag == 'y':
          refresh(curA, TABLES)

  except:
    print(invalid_message)

curA.close()
curB.close()
connection.close()
exit(0)

