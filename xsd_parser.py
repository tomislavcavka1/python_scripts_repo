import xmlschema
import json
from pprint import pprint
from itertools import chain, starmap
import argparse
import fileinput
import re
import random
import time
	
class RandomIdGenerator:
    def __init__(self):
        self.choices = list(range(350))
        random.shuffle(self.choices)

class XmlToSqlTypeConverter(object):

    def __init__(self):
        self.switchDict = {bool: self.boolean, int: self.integer, float: self.floatType, str: self.string}

    def convert_to_sql_type(self,type):
        return self.switchDict[type]()
    def boolean(self):
        return "BIT"
    def integer(self):
        return "INT"
    def floatType(self):
        return "DECIMAL(16,2)"
    def string(self):
        return "VARCHAR(100)"
   
class SqlGenerator:
    def __init__(self):
        self.table_id = ""
        self.create_table_query = ""
        self.insert_data_query = ""
        self.cached_tables = list()
        self.s = XmlToSqlTypeConverter()
        self.create_tables_file_name = 'create_tables_' + time.time() + '.sql'
        self.drop_tables_file_name = 'drop_tables_' + time.time() + '.sql'

    def generate_create_table_query(self, flatten_json_dict):
        self.delete_file_content(self.create_tables_file_name)
        self.delete_file_content(self.drop_tables_file_name)
        old_table_name = ""
        old_node_metadata = []
        references_table = ""
        for key, value in flatten_json_dict.items():
            node_metadata = key.split('_')
            if len(node_metadata) >= 3:
                if len(node_metadata) % 2 != 0:
                    node_metadata.insert(-1, 0)
                new_table_name = node_metadata[-3]
                column_name = str(node_metadata[-1]).replace("@", "").replace("-", "_")

                if len(self.cached_tables) > 0 and (new_table_name != old_table_name or \
                key == list(flatten_json_dict.keys())[-1]):
                    self.create_table_query = self.create_table_query.rstrip(",\n ")
                    if len(old_node_metadata) > 4 and references_table != "":
                        self.create_table_query += """,\n{}Id INT FOREIGN KEY REFERENCES {}({}Id)
                        """.format(references_table, references_table, references_table)
                    if self.create_table_query != "":
                        content_inserted_at_middle = bool(self.insert_line_in_the_middle(
                            new_table_name, 
                            self.create_table_query
                        ))
                        if not content_inserted_at_middle:   
                            self.create_table_query += ");\n"
                            print(self.create_table_query)
                            self.append_sql_to_script_file(self.create_table_query) 
                            # set database configuration
                            self.drop_table_statemens(old_table_name)
                    if key == list(flatten_json_dict.keys())[-1]:
                        self.reverse_sort_file(self.drop_tables_file_name)
                    self.create_table_query = ""
                    references_table = ""
                
                if not any(new_table_name in s for s in self.cached_tables):
                    if self.create_table_query != "":
                        self.create_table_query = ""
                    self.create_table_query = """\nCREATE TABLE {} (\n {} INT IDENTITY NOT NULL PRIMARY KEY, \n
                    """.format(new_table_name, new_table_name +"Id").rstrip() + "\n"
                    old_table_name = new_table_name
                    old_node_metadata = node_metadata
                    if len(node_metadata) > 4:
                        references_table = old_node_metadata[-5]

                if self.cached_tables.count(new_table_name + "_" + column_name) == 0:
                    self.create_table_query += column_name.lstrip() + " " + self.s.convert_to_sql_type(type(value)) + ",\n "
                self.cached_tables.append(new_table_name + "_" + column_name)


    def generate_insert_table_query(self, flatten_json_dict):
        old_node_key = ""
        old_table_name = ""
        old_node_metadata = ""
        insert_def_part = ""
        insert_value_part = ""
        new_node_key = ""
        data_key = ""
        column_name = ""
        cached_tables = list()
        for key, value in flatten_json_dict.items():
            node_metadata = key.split('_')            
            if len(node_metadata) >= 3:                
                if len(node_metadata) % 2 != 0:
                    node_metadata.insert(-1, 0)
                new_table_name = node_metadata[-3]
                column_name = str(node_metadata[-1]).replace("@", "").replace("-", "_")
                new_node_key = ""
                data_key = ""

                if len(node_metadata) >= 4:
                    for i in range(1, len(node_metadata) - 1):
                        new_node_key += ('_' if i!=1 else '') + str(node_metadata[i])
                    for i in range(1, len(node_metadata)):
                        data_key += ('_' if i!=1 else '') + str(node_metadata[i]).replace("@", "").replace("-", "_")

                if len(cached_tables) > 0 and (new_node_key != old_node_key or \
                key == list(flatten_json_dict.keys())[-1]):
                    insert_def_part = insert_def_part.rstrip(", ")
                    if len(old_node_metadata) > 4 and old_node_metadata[-5] != "":
                         insert_def_part += ", {}Id".format(old_node_metadata[-5])
                    insert_def_part += ")\n"
                    insert_value_part = insert_value_part.rstrip(", ")
                    if len(old_node_metadata) > 4 and old_node_metadata[-5] != "":
                        insert_value_part += ", (SELECT COUNT({}Id) FROM {})".format(old_node_metadata[-5], old_node_metadata[-5])
                    insert_value_part += "\n"
                    print(insert_def_part + insert_value_part)
                    # allow identity insert                    
                    self.append_sql_to_script_file(insert_def_part + insert_value_part)
                    if new_table_name != old_table_name or \
                    key == list(flatten_json_dict.keys())[-1]:
                        self.append_sql_to_script_file(self.identity_insert(old_table_name, "OFF"))
                    insert_def_part = ""
                    insert_value_part = ""
                    old_node_metadata = "" 
                
                matching = [s for s in cached_tables if new_node_key == s]
                if len(matching) == 0:
                    if insert_def_part != "":
                        insert_def_part = ""                        
                    if insert_value_part != "":
                        insert_value_part = ""
                    if new_table_name != old_table_name:
                        self.append_sql_to_script_file(self.identity_insert(new_table_name, "ON"))
                    old_node_key = new_node_key
                    old_table_name = new_table_name 
 
                    cached_tables.append(new_node_key)

                    insert_def_part = """\nINSERT INTO dbo.{} ({},
                    """.format(str(new_table_name), str(new_table_name)+"Id ").rstrip()
                    insert_value_part = "SELECT (SELECT COUNT({}Id) + 1 FROM {}), ".format(new_table_name, new_table_name)
                    if len(node_metadata) > 4:
                        old_node_metadata = node_metadata 

                if cached_tables.count(data_key) == 0:
                    insert_def_part += column_name + " , "
                    insert_value_part += "{}".format(self.prepare_value(value)) + " , "
                
    
    def prepare_value(self, value):
        if value != "-9" and isinstance(value, str):
            return "'" + value + "'"
        elif value == "-9":
            return "NULL"
        elif value == -9:
            return "NULL"
        else:
            return value

    def append_sql_to_script_file(self, sql):
        with open(self.create_tables_file_name, 'a', encoding="utf-8") as file:
            file.write(sql)
            file.close()
    
    def insert_line_in_the_middle(self, match_string, insert_string):
        insert = 0
        index = 0
        if len(insert_string.split(" ")) == 2:
            with open(self.create_tables_file_name, 'r+', encoding="utf-8") as file:
                contents = file.readlines()
                if len(contents) > 0:
                    start_search = False
                    for index, line in enumerate(contents):
                        match_str="CREATE TABLE {} (\n".format(match_string)
                        if match_str in line and \
                        not start_search:
                            start_search = True
                        if start_search:
                            if insert_string not in contents[index]:
                                insert = 1
                            else:
                                insert = 0
                            if ");" in contents[index]:
                                index = index
                                break

                    if bool(insert):
                       contents.insert(index - 2, insert_string+',\n')

                    file.seek(0)
                    file.writelines(contents)
            return insert


    def delete_file_content(self, file_name):
        with open(file_name, 'a') as file:
            file.seek(0)
            file.truncate()

    def identity_insert(self, table_name, on_off):
        identity_insert_on = """\n-- SET IDENTITY_INSERT to {}.  
SET IDENTITY_INSERT dbo.{} {};
GO\n""".format(on_off, table_name, on_off) 
        return identity_insert_on 

    def drop_table_statemens(self, table_name):
        with open(self.drop_tables_file_name, 'a') as file:
            file.write("DROP TABLE IF EXISTS dbo.{}; \n".format(table_name))
            file.close()


    def reverse_sort_file(self, file_name):
        with open(file_name, 'r+') as file:
            reversed_content = reversed(list(open(file_name)))
            self.delete_file_content(file_name)
            file.seek(0)
            file.writelines(reversed_content)

class XsdParser:
    """
    Class that parses XSD schema
    """

    def __init__(self, xml_schema, xml_document):
        self.xml_schema = xml_schema
        self.xml_document = xml_document


    def xsd_parse(self):
        """
        Function reads XSD shema and returns flatten JSON dictionary
        """
        xs = xmlschema.XMLSchema(
            self.xml_schema, converter=xmlschema.BadgerFishConverter)
        json_string = json.dumps(xs.to_dict(
            self.xml_document), ensure_ascii=False, indent=4).encode('utf-8')
        json_data = json_string.decode()
        json_obj = json.loads(json_data)
        return self.flatten_json_iterative_solution(json_obj)


    def flatten_json_iterative_solution(self, dictionary):
        """Flatten a nested json file"""

        def unpack(parent_key, parent_value):
            """Unpack one level of nesting in json file"""
            # Unpack one level only!!!
        
            if isinstance(parent_value, dict):
                for key, value in parent_value.items():
                    temp1 = parent_key + '_' + key
                    yield temp1, value
            elif isinstance(parent_value, list):
                i = 0 
                for value in parent_value:
                    temp2 = parent_key + '_'+ str(i) 
                    i += 1
                    yield temp2, value
            else:
                yield parent_key, parent_value    

            
        # Keep iterating until the termination condition is satisfied
        while True:
            # Keep unpacking the json file until all values are atomic elements (not dictionary or list)
            dictionary = dict(chain.from_iterable(starmap(unpack, dictionary.items())))
            # Terminate condition: not any value in the json file is dictionary or list
            if not any(isinstance(value, dict) for value in dictionary.values()) and \
            not any(isinstance(value, list) for value in dictionary.values()):
                break

        return dictionary

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Define paths to XSD and corresponding XML file.")
    parser.add_argument('--xsd_path', help="Define XSD path", required=True)
    parser.add_argument('--xml_path', help="Define XML path", required=True)
    args = parser.parse_args()    
    xsd_parser = XsdParser(xml_schema=args.xsd_path, xml_document=args.xml_path)
    flatten_json_dict = xsd_parser.xsd_parse()
    sql_generator = SqlGenerator()
    sql_generator.generate_create_table_query(flatten_json_dict=flatten_json_dict)
    sql_generator.generate_insert_table_query(flatten_json_dict=flatten_json_dict)

