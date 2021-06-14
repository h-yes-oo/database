from __future__ import print_function
from mysql.connector import connect
from executions import *
from table import TABLES
from messages import *

#connect with db in case it doesn't work at first time, use try except to ensure the connection
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

#make one dictionary cursor, one non-dictionary cursor for convenience
curA = connection.cursor(dictionary=True, buffered=True)
curB = connection.cursor(dictionary=False, buffered=True)

#print informative message and start program
print(info_message)
while(True):
  print(" ")
  #if input action is not an integer, go to except
  try:
    action = int(input(select_action))
    #if action not in 1~16 print invalid action message
    if action >= 17 or action <= 0:
      print(invalid_action)
    else:
      if action == 1:
        print_building(curA, curB)
      elif action == 2:
        print_performance_all(curA, curB)
      elif action == 3:
        print_audience_all(curA)
      elif action == 4:
        #get name, location and truncate to 200 letters
        name = input(get_building_name)
        if(len(name) > 200):
          name = name[0:200]
        location = input(get_building_location)
        if(len(location) > 200):
          location = location[0:200]
        try:
          #if capacity less than zero, print error message 
          capacity = int(input(get_building_capacity))
          if capacity <= 0:
            print(capacity_constraint)
            continue
          else:
            #if every input alright, insert it and commit
            insert_building(curA, name, location, capacity)
            connection.commit()
            print(insert_success_message("A building"))
        except:
          #if user input non-integer capacity, print invalid input
          print(invalid_input)
          continue

      elif action == 5:
        try:
          bid = int(input(get_building_id))
          if(check_by_id(curA, 'building', bid)):
            #if building with input id exist, delete and commit
            delete(curA, 'building', bid)
            connection.commit()
            print(delete_success_message("A building"))
          else:
            #if not exists, print message
            print(building_not_exist(bid))
        except:
          #if user input non-integer id, print invalid input
          print(invalid_input)

      elif action == 6:
        # get name and type from user and truncate to 200 letters
        name = input(get_performance_name)
        if(len(name) > 200):
          name = name[0:200]
        ptype = input(get_performance_type)
        if(len(ptype) > 200):
          ptype = ptype[0:200]
        try:
          price = int(input(get_performance_price))
          if price < 0:
            print(price_constraint)
            continue
          else:
            #if all input alright, insert it and commit
            insert_performance(curA, name, ptype, price)
            connection.commit()
            print(insert_success_message("A performance"))
        except:
          #if user input non-interger value for price, print invalid input
          print(invalid_input)
          continue

      elif action == 7:
        try:
          pid = int(input(get_performance_id))
          #check if performance with the id exists
          if(check_by_id(curA, 'performance', pid)):
            #if exist, delete and commit
            delete(curA, 'performance', pid)
            connection.commit()
            print(delete_success_message("A performance"))
          else:
            #if not exist, print not exist message
            print(performance_not_exist(pid))
        except:
          #if user input non-integer value for id, print invalid input
          print(invalid_input)
      
      elif action == 8:
        #get name and truncate
        name = input(get_audience_name)
        if(len(name) > 200):
          name = name[0:200]
        #get gender and check if equals to M or F
        gender = input(get_audience_gender)
        if gender != "M" and gender != "F":
          print(gender_constraint)
          continue
        try:
          #get age and check if bigger than 0
          age = int(input(get_audience_age))
          if age < 0:
            print(age_constraint)
            continue
          else:
            #if alright, insert and commit
            insert_audience(curA, name, gender, age)
            connection.commit()
            print(insert_success_message("An audience"))
        except:
          #if user input non-integer value for age, print message
          print(invalid_input)
          continue
      
      elif action == 9:
        try:
          aid = int(input(get_audience_id))
          if(check_by_id(curA, 'audience', aid)):
            #if audience with the id exist, delete and commit
            delete(curA, 'audience', aid)
            connection.commit()
            print(delete_success_message("An audience"))
          else:
            #else, print not exist message
            print(audience_not_exist)
        except:
          #if user input non-integer value for id, print message
          print(invalid_input)
      
      elif action == 10:
        try:
          bid = int(input(get_building_id))
          #if building not exist, print message
          if not check_by_id(curA, 'building', bid):
            print(building_not_exist(bid))
            continue
          pid = int(input(get_performance_id))
          #if performance not exist, print message
          if not check_by_id(curA, 'performance', pid):
            print(performance_not_exist(pid))
            continue
          #if it is able to assign the performance to building, do it and commit
          if(assign_p_to_b(curA, pid, bid)):
            print(assign_success)
            connection.commit()
        except:
          print(invalid_input)
      
      elif action == 11:
        try:
          pid = int(input(get_performance_id))
          # if performance not assigned to any building, print message
          if not check_building_assigned(curA, pid):
            print(performance_not_assigned(pid))
            continue
          aid = int(input(get_audience_id))
          seat_list = list(map(int, input(get_seat_number).split(',')))
          #check if all seats can be reserved
          if(can_reserve(curB, seat_list, pid)):
            for seat in seat_list:
              #reserve each seat and commit
              reserve(curA, pid, aid, seat)
              connection.commit()
            #get price with respect to audience's age
            price = get_ticket_price(curB, pid, aid)
            #multiply the price with the number of ticket being reserved
            total_price = round(price * len(seat_list))
            #print success message and price
            print(book_success)
            print(total_ticket_price(total_price))
        except:
          print(invalid_input)

      elif action == 12:
        try:
          bid = int(input(get_building_id))
          #check if building with the id exist and print
          if check_by_id(curA, 'building', bid):
            print_performance_of_building(curA, curB, bid)
          else:
            print(building_not_exist(bid))
        except:
          print(invalid_input)

      elif action == 13:
        try:
          pid = int(input(get_performance_id))
          #check if performance with the id exist and print
          if check_by_id(curA, 'performance', pid):
            print_audience_of_performance(curA, pid)
          else:
            print(performance_not_exist(pid))
        except:
          print(invalid_input)
      
      elif action == 14:
        try:
          pid = int(input(get_performance_id))
          #check if performance with the id exist
          if check_by_id(curA, 'performance', pid):
            #check if performance with the id is assigned to any building
            if check_building_assigned(curA, pid):
              print_seat_of_performance(curB, pid)
            else:
              print(performance_not_assigned(pid))
          else:
            print(performance_not_exist(pid))
        except:
          print(invalid_input)

      elif action == 15:
        print(bye)
        break

      elif action == 16:
        #ensure user about deleting everything
        flag = input(refresh_confirm)
        if flag == 'y':
          refresh(curA, TABLES)

  #if non integer input came, print invalid message
  except:
    print(invalid_action)

curA.close()
curB.close()
connection.close()
exit(0)

