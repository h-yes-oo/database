import sys
import lark
from lark import Lark, Transformer
from berkeleydb import db
import os
import json

# to reuse prompt message
prompt = "DB_2015-16227> "

#messages
def CreateTableSuccess(tableName):
  return f"'{tableName}' table is created"
CharLengthError = "Char length should be over 0"
DuplicatePrimaryKeyDefError = "Create table has failed: primary key definition is duplicated"
DuplicateColumnDefError = "Create table has failed: column definition is duplicated"
def NonExistingColumnDefError(colName):
  return f"Create table has failed: '{colName}' does not exists in column definition"
TableExistenceError = "Create table has failed: table with the same name already exists"

ReferenceTypeError = "Create table has failed: foreign key references wrong type"
ReferenceNonPrimaryKeyError = "Create table has failed: foreign key references non primary key column"
ReferenceColumnExistenceError = "Create table has failed: foreign key references non existing column"
ReferenceTableExistenceError = "Create table has failed: foreign key references non existing table"

def DropSuccess(tableName):
  return f"'{tableName}' table is dropped"
def DropReferencedTableError(tableName):
  return f"Drop table has failed: '{tableName}' is referenced by other table"

NoSuchTable = "No such table"

# make transformer to print appropriate message for each query
class MyTransformer(Transformer):
  def command(self, items):
    if not isinstance(items[0], list):
      exit()
    return items[0]

  def query_list(self, items):
    return items

  def create_table_query(self, items):
    try:
      info_list = items[2:] #get rid of CREATE TABLE
      table_name = info_list[0]
      file_name = f"db/{table_name}.db"
      #check if the file already exists
      if os.path.isfile(file_name):
        raise Exception(TableExistenceError)
      elem_list = info_list[1]
      #initialize variables
      find_primary_key = False
      primary_key = []
      foreign_key = []
      referencing = []
      referenced = []
      col_dict = {}
      #processing table_element_list
      for elem in elem_list:
        if elem[0] == 'primary key':
          #if already find primary key, raise error
          if find_primary_key:
            raise Exception(DuplicatePrimaryKeyDefError)
          else:
            find_primary_key = True
            primary_key = elem[1]
        #process foreign key after get all column information
        elif elem[0] == 'foreign key':
          continue
        #store all column names and infos into a dict
        else:
          #if already have the same column, raise error
          if elem[0] in col_dict:
            raise Exception(DuplicateColumnDefError)
          else:
            #if char length smaller than 1, raise error
            if elem[1]['type'] == 'error':
              raise Exception(CharLengthError)
            else:
              col_dict[elem[0]] = elem[1]
      #process all foreign keys
      for elem in elem_list:
        if elem[0] == 'foreign key':
          fk_list = elem[1]
          referenced_table = elem[2]
          referenced_col_list = elem[3]
          # check if foreign key is one of columns
          for f in fk_list:
            if f not in col_dict:
              raise Exception(NonExistingColumnDefError(f))
          # check if num of foreign keys and num of referenced cols is the same
          if (len(fk_list) != len(referenced_col_list)):
            Exception(ReferenceTypeError)
          ref_file_name = f"db/{referenced_table}.db"
          # check if the referenced_table exists
          if not os.path.isfile(ref_file_name):
            raise Exception(ReferenceTableExistenceError)
          else:
            refDB = db.DB()
            refDB.open(ref_file_name, dbtype=db.DB_HASH)
            ref_pk_list = json.loads(refDB.get(b"primary_key"))
            ref_col_list = json.loads(refDB.get(b"columns"))
            # check if referenced table has foreign key
            for rf_col in referenced_col_list:
              if rf_col not in ref_col_list:
                raise Exception(ReferenceColumnExistenceError)
            # check if foreign key is primary key of referenced table
            for rf_col in referenced_col_list:
              if rf_col not in ref_pk_list:
                raise Exception(ReferenceNonPrimaryKeyError)
            # check if foreign key contains all primary keys of referenced table
            if len(referenced_col_list) != len(ref_pk_list):
              raise Exception(ReferenceNonPrimaryKeyError)
            else:
              for i in range(len(fk_list)):
              # check if fk_list[i] and referenced_col_list[i] has the same type
                ori_type = json.loads(refDB.get(referenced_col_list[i].encode('utf-8')))['type']
                fk_type = col_dict[fk_list[i]]['type']
                if ori_type != fk_type:
                  raise Exception(ReferenceTypeError)
            #ready to reference
            #add referenced_table to referencing list of this new table
            referencing.append(referenced_table)
            #add this new table to referenced list of the referenced_table
            rf_by = json.loads(refDB.get(b"referenced"))
            rf_by.append(table_name)
            refDB.put(b"referenced", json.dumps(rf_by).encode('utf-8'))
            #add all foreign keys to foreign_key
            for fk in fk_list:
              foreign_key.append(fk)
            refDB.close()
      #check if primary_key and foreign_key is in column list and change their properties
      for p in primary_key:
        if p in col_dict:
          col_dict[p]['null'] = 'N'
          col_dict[p]['key'] = 'PRI'
        else:
          raise Exception(NonExistingColumnDefError(p))

      for f in foreign_key:
        if f in col_dict:
          if col_dict[f]['key'] == '':
            col_dict[f]['key'] = 'FOR'
          else:
            col_dict[f]['key'] += '/FOR'
        else:
          raise Exception(NonExistingColumnDefError(f))
      #encode variables and put to database
      encoded_p = json.dumps(primary_key).encode('utf-8')
      encoded_f = json.dumps(foreign_key).encode('utf-8')

      myDB = db.DB()
      myDB.open(file_name, dbtype=db.DB_HASH, flags=db.DB_CREATE)
      for key in col_dict.keys():
        myDB.put(key.encode('utf-8'), json.dumps(col_dict[key]).encode('utf-8'))
      myDB.put(b"primary_key",encoded_p)
      myDB.put(b"foreign_key",encoded_f)
      myDB.put(b"referenced", json.dumps(referenced).encode('utf-8'))
      myDB.put(b"referencing", json.dumps(referencing).encode('utf-8'))
      myDB.put(b"columns", json.dumps(list(col_dict.keys())).encode('utf-8'))
      myDB.close()
      print(prompt, CreateTableSuccess(table_name), sep="")
    except Exception as e:
      print(prompt,e,sep='')


  def drop_table_query(self, items):
    try:
      table_name = items[2]
      file_name = f"db/{table_name}.db"
      #check if the table exists
      if not os.path.isfile(file_name):
        raise Exception(NoSuchTable)
      else:
        #get the table's referenced and referencing
        myDB = db.DB()
        myDB.open(file_name, dbtype=db.DB_HASH)
        referenced_by = json.loads(myDB.get(b"referenced"))
        referencing = json.loads(myDB.get(b"referencing"))
        myDB.close()
        #if no table is referencing it, can drop
        if len(referenced_by) == 0:
          #for all table which this table is referencing,
          #remove this table's name from their referenced list
          for rf in referencing:
            rf_file = f"db/{rf}.db"
            rfDB = db.DB()
            rfDB.open(rf_file, dbtype=db.DB_HASH)
            referenced = json.loads(rfDB.get(b"referenced"))
            referenced.remove(table_name)
            rfDB.put(b"referenced", json.dumps(referenced).encode('utf-8'))
            rfDB.close()
          #delete the database file
          os.remove(file_name)
          print(prompt, DropSuccess(table_name), sep="")
        #if the table is referenced by more than one table, cannot drop
        else:
          raise Exception(DropReferencedTableError(table_name))
    except Exception as e:
      print(prompt, e, sep='')

  #show all tables by their file name
  def show_tables_query(self, items):
    print('----------------')
    tables = os.listdir('../PRJ1-3_2015-16227/db')
    for t in tables:
      print(t[:-3])
    print('----------------')

  def desc_query(self, items):
    try:
      table_name = items[1]
      file_name = f"db/{table_name}.db"
      #check if the table exists
      if not os.path.isfile(file_name):
        raise Exception(NoSuchTable)
      else:
        myDB = db.DB()
        myDB.open(file_name, dbtype=db.DB_HASH)
        print("------------------------------------------------------------")
        print(f"table_name {table_name}")
        print('%-20s%-20s%-20s%-20s' % ('column_name', 'type', 'null', 'key'))
        #get column list and iterate
        col_names = json.loads(myDB.get(b'columns'))
        for key in col_names:
          val = json.loads(myDB.get(key.encode('utf-8')))
          print('%-20s%-20s%-20s%-20s' % (key,val['type'], val['null'], val['key']))
        myDB.close()
        print("-----------------------------------------------------------")
    except Exception as e:
      print(prompt,e,sep='')

  def select_query(self, items):
    print(prompt,"'SELECT' requested",sep='')
  def insert_query(self, items):
    print(prompt,"'INSERT' requested",sep='')
  def delete_query(self, items):
    print(prompt,"'DELETE' requested",sep='')

  def table_element_list(self, items):
    elem_list = items[1:-1]  # get rid of LP and RP
    return elem_list

  def table_element(self, items):
    return items[0]

  def column_definition(self, items):
    dict = {}
    dict['type'] = items[1]
    #if not null constraint, set null to N
    if len(items) > 2:
      dict['null'] = 'N'
    else:
      dict['null'] = 'Y'
    dict['key'] = ''
    return (items[0], dict)

  def data_type(self, items):
    length = len(items)
    if (length == 4):
      #if char length smaller than 1, set type to 'error'
      if int(items[2]) <= 0:
        return 'error'
    result = ''
    for i in range(length):
      result += str(items[i])
    return result.lower()

  def table_constraint_definition(self, items):
    return items[0]

  def primary_key_constraint(self, items):
    return ['primary key', items[2]]

  def referential_constraint(self, items):
    col_list = items[2]
    referenced_table = items[4]
    referenced_col_list = items[5]
    return ['foreign key', col_list, referenced_table, referenced_col_list ]

  def column_name_list(self, items):
    return list(items[1:-1])
  #table name to lower case
  def table_name(self, t):
    (t,) = t
    return t.lower()
  #column name to lower case
  def column_name(self, c):
    (c,) = c
    return c.lower()
  def IDENTIFIER(self, s):
    return str(s)


if __name__ == '__main__':

  # make sql_parser with grammar.lark file
  with open('../PRJ1-3_2015-16227/grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="standard")

  #get input until the program ended
  while(True):
    #get input with prompt message
    data_input = input(prompt)
    #if command 'exit' is given, end the program
    if(data_input == 'exit'):
        sys.exit()
    #get additional input until it ends with semicolon
    while len(data_input) == 0 or data_input[-1] != ';':
      #to prevent error, add white space to data_input
      data_input += ' '
      data_input += input()
    #split the input by semicolon
    input_list = list(data_input.split(';'))
    #exclude the last element of input_list since it is empty string
    for i in range(len(input_list) - 1):
      #restore semicolon in the end of the command
      command = input_list[i] + ';'
      #using try ... except, process the syntax error
      try:
        result = sql_parser.parse(command)
        MyTransformer().transform(result)
      except Exception as e:
        print(prompt,"Syntax error",sep='')
        break

