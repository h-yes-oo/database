#informative message about prompt
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
============================================================'''

invalid_action = "Invalid action"
invalid_input = "Invalid input"

select_action = "Select your action: "

get_building_name = "Building name: "
get_building_location = "Building location: "
get_building_capacity = "Building capacity: "
get_building_id = "Building ID: "

get_performance_name = "Performance name: "
get_performance_type = "Performance type: "
get_performance_price = "Performance price: "
get_performance_id = "Performance ID: "

get_audience_name = "Audience name: "
get_audience_gender = "Audience gender: "
get_audience_age = "Audience age: "
get_audience_id = "Audience ID: "

get_seat_number = "Seat number: "

capacity_constraint = "Capacity should be more than 0"
price_constraint = "Price should be 0 or more"
gender_constraint = "Gender should be 'M' or 'F'"
age_constraint = "Age should be 0 or more"

bye = "Bye!"
refresh_confirm = "Every table and data will be deleted. Go on ? (y/n) : "

assign_success = "Successfully assign a performance"
book_success = "Successfully book a performance."

def total_ticket_price(total_price):
  return "Total ticket price is {:,}".format(total_price)

def insert_success_message(what):
  return "{} is successfully inserted".format(what)

def delete_success_message(what):
  return "{} is successfully removed".format(what)

def building_not_exist(bid):
  return "Building {} doesn’t exist".format(bid)

def performance_not_exist(pid):
  return "Performance {} doesn’t exist".format(pid)

def audience_not_exist(aid):
  return "Audience {} doesn’t exist".format(aid)

def performance_not_assigned(pid):
  return "Performance {} isn't assigned".format(pid)

