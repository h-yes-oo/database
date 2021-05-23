import sys
from lark import Lark, Transformer
#to reuse prompt message
prompt = "DB_2015-16227> "
#make transformer to print appropriate message for each query
class MyTransformer(Transformer):
  def create_table_query(self, items):
    print(prompt,"'CREATE TABLE' requested",sep='')
  def drop_table_query(self, items):
    print(prompt,"'DROP TABLE' requested",sep='')
  def select_query(self, items):
    print(prompt,"'SELECT' requested",sep='')
  def insert_query(self, items):
    print(prompt,"'INSERT' requested",sep='')
  def desc_query(self, items):
    print(prompt,"'DESC' requested",sep='')
  def delete_query(self, items):
    print(prompt,"'DELETE' requested",sep='')
  def show_tables_query(self, items):
    print(prompt,"'SHOW TABLES' requested",sep='')
#make sql_parser with grammar.lark file
with open('grammar.lark') as file:
  sql_parser = Lark(file.read(), start="command", lexer="standard")

if __name__ == '__main__':
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
      except:
        print(prompt,"Syntax error",sep='')
        break