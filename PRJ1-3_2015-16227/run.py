import sys
import lark
from lark import Lark, Transformer
from berkeleydb import db
import os
import json
from exceptions import *
import datetime
from prettytable import PrettyTable

# to reuse prompt message
prompt = "DB_2015-16227> "

#messages
def CreateTableSuccess(tableName):
  return f"'{tableName}' table is created"

def DropSuccess(tableName):
  return f"'{tableName}' table is dropped"



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
        raise TableExistenceError()
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
            raise DuplicatePrimaryKeyDefError()
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
            raise DuplicateColumnDefError()
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
              raise NonExistingColumnDefError(f)
          # check if num of foreign keys and num of referenced cols is the same
          if (len(fk_list) != len(referenced_col_list)):
            raise ReferenceTypeError()
          ref_file_name = f"db/{referenced_table}.db"
          # check if the referenced_table exists
          if not os.path.isfile(ref_file_name):
            raise ReferenceTableExistenceError()
          else:
            refDB = db.DB()
            refDB.open(ref_file_name, dbtype=db.DB_HASH)
            ref_pk_list = json.loads(refDB.get(b"primary_key"))
            ref_col_list = json.loads(refDB.get(b"columns"))
            # check if referenced table has foreign key
            for rf_col in referenced_col_list:
              if rf_col not in ref_col_list:
                raise ReferenceColumnExistenceError()
            # check if foreign key is primary key of referenced table
            for rf_col in referenced_col_list:
              if rf_col not in ref_pk_list:
                raise ReferenceNonPrimaryKeyError()
            # check if foreign key contains all primary keys of referenced table
            if len(referenced_col_list) != len(ref_pk_list):
              raise ReferenceNonPrimaryKeyError()
            else:
              for i in range(len(fk_list)):
              # check if fk_list[i] and referenced_col_list[i] has the same type
                ori_type = json.loads(refDB.get(referenced_col_list[i].encode('utf-8')))['type']
                fk_type = col_dict[fk_list[i]]['type']
                if ori_type != fk_type:
                  raise ReferenceTypeError()
            #ready to reference
            #add referenced_table to referencing list of this new table
            referencing.append(referenced_table)
            #add this new table to referenced list of the referenced_table
            rf_by = json.loads(refDB.get(b"referenced"))
            rf_by.append(table_name)
            refDB.put(b"referenced", json.dumps(rf_by).encode('utf-8'))
            #add all foreign key information to foreign_key
            ordered_fk_list = ['' for _ in range(len(fk_list))]
            for idx in range(len(fk_list)):
              pk = referenced_col_list[idx]
              ori_idx = ref_pk_list.index(pk)
              ordered_fk_list[ori_idx] = fk_list[idx]

            foreign_key.append([ordered_fk_list, referenced_table, ref_pk_list])
            refDB.close()
      #check if primary_key and foreign_key is in column list and change their properties
      for p in primary_key:
        if p in col_dict:
          col_dict[p]['null'] = 'N'
          col_dict[p]['key'] = 'PRI'
        else:
          raise NonExistingColumnDefError(p)

      for fk_info in foreign_key:
        for f in fk_info[0]:
          if f in col_dict:
            if col_dict[f]['key'] == '':
              col_dict[f]['key'] = 'FOR'
            else:
              col_dict[f]['key'] += '/FOR'
          else:
            raise NonExistingColumnDefError(f)
      '''
      for f in foreign_key:
        if f in col_dict:
          if col_dict[f]['key'] == '':
            col_dict[f]['key'] = 'FOR'
          else:
            col_dict[f]['key'] += '/FOR'
        else:
          raise NonExistingColumnDefError(f)
          '''
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
        raise NoSuchTable()
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
          raise DropReferencedTableError(table_name)
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
        raise NoSuchTable()
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
    cols, col_nick = items[1]
    table_expression = items[2]
    where = table_expression[0]
    tables, nickname  = table_expression[1][0]
    #check if all tables exists
    existing_tables = os.listdir('../PRJ1-3_2015-16227/db')
    existing_tables = list(map(lambda x : x[:-3], existing_tables))
    for table in tables:
      if table not in existing_tables:
        raise SelectTableExistenceError(table)
    columns = []
    # if col list is *, add all columns of tables
    if len(cols) == 0:
      col_table = []
      for table in tables:
        myDB = db.DB()
        myDB.open(f"db/{table}.db", dbtype=db.DB_HASH)
        col_names = json.loads(myDB.get(b'columns'))
        for col in col_names:
          if col in columns:
            dup_idx = columns.index(col)
            columns[dup_idx] = f"{col_table[dup_idx]}.{col}"
            columns.append(f"{table}.{col}")
            col_table.append(table)
          else:
            columns.append(col)
            col_table.append(table)
        myDB.close()
    else:
    #if col list is given, check if the cols are all right and sort by tables
      for table in tables:
        myDB = db.DB()
        myDB.open(f"db/{table}.db", dbtype=db.DB_HASH)
        col_names = json.loads(myDB.get(b'columns'))
        for col in cols:
          if len(col.split('.')) > 1:
            if col.split('.')[0] == table:
              if col.split('.')[1] in col_names:
                columns.append(col)
              else:
                raise SelectColumnResolveError(col)
          else:
            if col in col_names:
              columns.append(col)
        myDB.close()
      for col in cols:
        if col not in columns:
          raise SelectColumnResolveError(col)
      #if duplicate column name exist, ambiguous
      if len(cols) != len(columns):
        raise SelectColumnResolveError(col)
    
    if where:
    #with where clause
      print(table_expression[1][1])
    else:
    #without where cluase
      result = PrettyTable(columns)
      rows = []
      for table in tables:
        infos = []
        myDB = db.DB()
        myDB.open(f"db/{table}.db", dbtype=db.DB_HASH)
        col_names = json.loads(myDB.get(b'columns'))
        info_list = list(map(lambda col: bytes(col, encoding="utf-8"), col_names))
        info_list += [b'primary_key', b'foreign_key', b'referencing', b'referenced', b'columns']
        cursor = myDB.cursor()
        data = cursor.first()
        while data:
          key, value = data
          val_dict = json.loads(value)
          if key not in info_list:
            info = []
            for col in columns:
              if len(col.split('.')) > 1:
                t_name, col_name = col.split('.')
                if t_name != table:
                  info.append(None)
                else:
                  if val_dict[col_name] is None:
                    info.append('null')
                  else:
                    info.append(val_dict[col_name])
              else:
                if col in val_dict:
                  if val_dict[col] is None:
                    info.append('null')
                  else:
                    info.append(val_dict[col])
                else:
                  info.append(None)
            infos.append(info)
          data = cursor.next()
        myDB.close()
        rows.append(infos)
      for i in range(1,len(rows)):
        new_row = []
        for j in range(len(rows[i])):
          for row in rows[i-1]:
            r = [x if y is None else y for x,y in zip(rows[i][j], row)]
            new_row.append(r)
        rows[i] = new_row
      print(rows)
      for r in rows[-1]:
       result.add_row(r)
      print(result)


  def select_list(self, items):
    col_sel = []
    col_nick = {}
    for i in items:
      if i[0]:
        col_nick[i[-1]] = i[1]
      col_sel.append(i[1])
    return (col_sel, col_nick)

  def selected_column(self, items):
    case = len(items)
    if case == 1:
      #only column_name given
      return (False, items[0])
    elif case  == 2:
      #table name and column name given
      return (False, f"{items[0]}.{items[1]}")
    elif case == 3:
      #column name and nickname given
      return (True, items[0], items[-1])
    elif case == 4:
      #table name and column name nickname given
      return (True, f"{items[0]}.{items[1]}", items[-1])

  def table_expression(self, items):
    return (len(items) >  1 , items)

  def from_clause(self, items):
    return items[-1]

  def table_reference_list(self, items):
    tables = []
    nickname = {}
    for i in items:
      if i[0]:
        nickname[i[-1]] = i[1]
      tables.append(i[1])
    return(tables, nickname)

  def referred_table(self, items):
    if len(items) > 1:
      return(True,items[0],items[-1])
    else:
      return(False, items[0])

  def insert_query(self, items):
    table_name = items[2]
    file_name = f"db/{table_name}.db"
    if not os.path.isfile(file_name):
      raise NoSuchTable()
    else:
      myDB = db.DB()
      myDB.open(file_name, dbtype=db.DB_HASH)

      original_cols = json.loads(myDB.get(b'columns'))
      column_name_list = original_cols
      insert_info = items[-1]
      if len(insert_info) > 1:
        column_name_list = insert_info[0]
        for col in column_name_list:
          if col not in original_cols:
            raise InsertColumnExistenceError(col)
      value_list = insert_info[-1]

      if len(column_name_list) != len(value_list):
        raise InsertTypeMismatchError()

      data_dict = {'referencing': [], 'referenced': []}

      for i in range(len(value_list)):
        col_name = column_name_list[i]
        val = value_list[i]
        col_info = json.loads(myDB.get(col_name.encode('utf-8')))
        #check if nullable
        nullable = True
        if col_info['null'] == 'N':
          nullable = False
        #if value is null, check if nullable and store value into dict
        if val is None:
          if nullable:
            data_dict[col_name] = None
          else:
            raise InsertColumnNonNullableError(col_name)
        #if value is not null, check if type is right
        else:
          required_type = col_info['type']
          if required_type == 'int':
            if isinstance(val, int):
              data_dict[col_name] = val
            else:
              raise InsertTypeMismatchError()
          elif required_type == 'date':
            if isinstance(val, datetime.date):
              data_dict[col_name] = val.strftime('%Y-%m-%d')
            else:
              raise InsertTypeMismatchError()
          else:
            #if required type is string
            if isinstance(val, str):
              str_len = int(required_type[5:-1])
              #only store to max size of string
              data_dict[col_name] = val[0:str_len]
            else:
              raise InsertTypeMismatchError()

      #find if foreign key values exist in the referenced table
      foreign_key = json.loads(myDB.get(b'foreign_key'))
      for fk_info in foreign_key:
        fk_list = fk_info[0]
        referenced = fk_info[1]
        ori_cols = fk_info[2]

        rf_file_name = f"db/{referenced}.db"
        rfDB = db.DB()
        rfDB.open(rf_file_name, dbtype=db.DB_HASH)

        pk_idx = []
        for fk_idx in range(len(fk_list)):
          pk_idx.append(data_dict[fk_list[fk_idx]])

        #if all the foreign keys are null, not have to find, else, find the data from referenced table
        if not all(pk is None for pk in pk_idx):
          rf_pidx = json.dumps(pk_idx).encode('utf-8')
          referenced_data = rfDB.get(rf_pidx)
          if referenced_data is None:
            raise InsertReferentialIntegrityError()
          else:
            ori_referencing = data_dict['referencing']
            ori_referencing.append((referenced, json.loads(rf_pidx)))
            data_dict['referencing'] = ori_referencing

        rfDB.close()


      #make key to store in berkely db
      primary_idx = []
      primary_key = json.loads(myDB.get(b'primary_key'))

      for pk in primary_key:
        primary_idx.append(data_dict[pk])
      pidx = json.dumps(primary_idx).encode('utf-8')

      #find if data with the same primary key values exists
      cursor = myDB.cursor()
      data = cursor.first()
      while data:
        key, value = data
        if key == pidx:
          raise InsertDuplicatePrimaryKeyError
        data = cursor.next()

      #insert new data to database
      myDB.put(pidx, json.dumps(data_dict).encode('utf-8'))
      for info in data_dict['referencing']:
        rf_table = info[0]
        rf_pidx = json.dumps(info[1]).encode('utf-8')
        rfDB = db.DB()
        rfDB.open(f"db/{rf_table}.db", dbtype=db.DB_HASH)
        rf_dict = json.loads(rfDB.get(rf_pidx))
        rf_referenced = rf_dict['referenced']
        rf_referenced.append((table_name,primary_idx))
        rf_dict['referenced'] = rf_referenced
        rfDB.put(rf_pidx, json.dumps(rf_dict).encode('utf-8'))
        rfDB.close()

      myDB.close()
      print(prompt,"The row is inserted",sep='')



  def insert_columns_and_sources(self, items):
    return items

  def value_list(self, items):
    return items[2:-1]

  def value(self, items):
    type = items[0].type
    if type == 'INT':
      return int(items[0])
    elif type == 'DATE':
      return datetime.datetime.strptime(items[0],'%Y-%m-%d')
    elif type == 'STR':
      return items[0][1:-1]
    elif type == 'NULL':
      return None

  def comparable_value(self, items):
    return items[0]

  def delete_query(self, items):
    table_name = items[2]
    file_name = f"db/{table_name}.db"
    # check if the file already exists
    if not os.path.isfile(file_name):
      raise NoSuchTable()
    else:
      myDB = db.DB()
      myDB.open(file_name, dbtype=db.DB_HASH)
      #info_list contains each key which is not a key for instance
      columns = json.loads(myDB.get(b'columns'))
      info_list = list(map(lambda col : bytes(col, encoding="utf-8"), columns))
      info_list += [b'primary_key',b'foreign_key',b'referencing',b'referenced',b'columns']
      #to count
      yes_count = 0
      no_count = 0
      #referenced information
      referenced = json.loads(myDB.get(b'referenced'))
      ref_info = {}
      ref_colnames = {}
      for table in referenced:
        rfDB = db.DB()
        rfDB.open(f"db/{table}.db", dbtype=db.DB_HASH)
        rf_fk_info = json.loads(rfDB.get(b'foreign_key'))
        for fk_info in rf_fk_info:
          if fk_info[1] == table_name:
            fk_list = fk_info[0]
            can_delete = True
            for col in fk_list:
              if json.loads(rfDB.get(col.encode("utf-8")))['null'] == 'N':
                can_delete = False
                break
            #store info boolean representing if all referenced cols are nullable
            ref_info[table] = can_delete
            ref_colnames[table] = fk_list
            break
        rfDB.close()
      #no where clause, delete all
      if (len(items) == 3):
        cursor = myDB.cursor()
        data = cursor.first()
        while data:
          key, value = data
          if key not in info_list:
            #find if this data can be deleted
            referenced = json.loads(value)['referenced']
            can_delete = True
            for rf in referenced:
              rf_table = rf[0]
              if ref_info[rf_table] == False:
                can_delete = False
            if can_delete:
              #delete this data from referenced
              for rf in referenced:
                rf_table = rf[0]
                rf_key = json.dumps(rf[1]).encode('utf-8')
                rfDB = db.DB()
                rfDB.open(f"db/{rf_table}.db", dbtype=db.DB_HASH)
                rf_dict = json.loads(rfDB.get(rf_key))
                #remove this value from 'referencing' of referenced value
                rf_referencing = rf_dict['referencing']
                def checkThis(x):
                  return not (json.dumps(x[1]).encode('utf-8') == key and x[0] == table_name)
                rf_referencing = list(filter(checkThis, rf_referencing))
                rf_dict['referencing'] = rf_referencing
                #change all referenced col values to null
                ref_col = ref_colnames[rf_table]
                for col in ref_col:
                  rf_dict[col] = None
                #store edited value
                rfDB.put(rf_key, json.dumps(rf_dict).encode('utf-8'))
                rfDB.close()

              #delete this data from referencing
              referencing = json.loads(value)['referencing']
              for rfc in referencing:
                rfc_table = rfc[0]
                rfc_key = json.dumps(rfc[1]).encode('utf-8')
                rfcDB = db.DB()
                rfcDB.open(f"db/{rfc_table}.db", dbtype=db.DB_HASH)
                rfc_dict = json.loads(rfcDB.get(rfc_key))
                #remove this value from 'referenced' of referencing value
                rfc_referenced = rfc_dict['referenced']
                def checkThis(x):
                  return not (json.dumps(x[1]).encode('utf-8') == key and x[0] == table_name)
                rfc_referenced = list(filter(checkThis, rfc_referenced))
                rfc_dict['referenced'] = rfc_referenced
                #store edited value
                rfcDB.put(rfc_key, json.dumps(rfc_dict).encode('utf-8'))
                rfcDB.close()
              #delete this value from database
              myDB.delete(key)
              yes_count += 1
            else:
              #cannot delete this value due to referential integrity
              no_count += 1
          data = cursor.next()
      #with where clause
      else:
        cursor = myDB.cursor()
        data = cursor.first()
        while data:
          key, value = data
          if key not in info_list:
            print(data)
            myDB.delete(key)
          data = cursor.next()
      myDB.close()
      print(prompt,f"{yes_count} row(s) are deleted", sep='')
      if no_count > 0:
        print(prompt, f"{no_count} row(s) are not deleted due to referential integrity", sep='')


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
      #if char length smaller than 1, rais CharLenghError
      if int(items[2]) <= 0:
        raise CharLengthError()
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
      except lark.exceptions.UnexpectedInput:
        print(prompt,"Syntax error",sep='')
      #for char length error
      except lark.exceptions.VisitError as e:
        print(prompt + str(e.__context__))
      except SimpleDatabaseError as e:
        print(prompt + str(e))

