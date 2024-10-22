import json
import re
from pymongo import MongoClient
import sqlite3

# Define the AST Node structure
class Node:
    def __init__(self, node_type, value=None, left=None, right=None):
        self.type = node_type  # "operator" or "operand"
        self.value = value      # Condition (for operand) or Operator ("AND"/"OR")
        self.left = left        # Left child (for operator nodes)
        self.right = right      # Right child (for operator nodes)

# Function to create AST from rule string
def create_rule(rule_string):
    # Clean up and break rule into tokens for parsing
    rule_string = rule_string.strip().replace('(', '').replace(')', '')
    tokens = re.split(r"(\band\b|\bor\b)", rule_string, flags=re.IGNORECASE)

    # Recursive function to parse tokens
    def parse(tokens):
        if len(tokens) == 1:
            condition = tokens[0].strip()
            return Node(node_type="operand", value=condition)

        operator = tokens[1].strip().lower()
        left = parse([tokens[0].strip()])
        right = parse([tokens[2].strip()])
        
        return Node(node_type="operator", value=operator, left=left, right=right)
    
    return parse(tokens)

# Function to serialize AST to JSON
def serialize_ast(node):
    if node is None:
        return None
    return {
        "type": node.type,
        "value": node.value,
        "left": serialize_ast(node.left),
        "right": serialize_ast(node.right),
    }

# Updated evaluate_rule function to handle Node objects
def evaluate_rule(node, data):
    if node.type == "operand":
        field, operator, value = re.split(r"(\>|\<|=)", node.value)
        field = field.strip()
        value = value.strip().strip("'")
        
        user_value = data.get(field)
        
        if operator == ">":
            return user_value > int(value)
        elif operator == "<":
            return user_value < int(value)
        elif operator == "=":
            return user_value == value
    
    elif node.type == "operator":
        left_result = evaluate_rule(node.left, data)
        right_result = evaluate_rule(node.right, data)
        
        if node.value.lower() == "and":
            return left_result and right_result
        elif node.value.lower() == "or":
            return left_result or right_result
    
    return False

# Function to convert a dictionary representation of AST back to Node objects
def deserialize_ast(ast_dict):
    if ast_dict is None:
        return None
    node = Node(node_type=ast_dict['type'], value=ast_dict.get('value'))
    node.left = deserialize_ast(ast_dict.get('left'))
    node.right = deserialize_ast(ast_dict.get('right'))
    return node

# Function to connect to MongoDB and store rule
def store_rule_mongo(rule_string, ast):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['rule_engine']
    rules_collection = db['rules']

    rule_data = {
        "rule_string": rule_string,
        "ast": ast
    }
    
    rules_collection.insert_one(rule_data)
    print("Stored in MongoDB:", rule_data)

# Function to connect to SQLite and store rule
def store_rule_sqlite(rule_string, ast):
    conn = sqlite3.connect('rules.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS rules (
        id INTEGER PRIMARY KEY,
        rule_string TEXT,
        ast TEXT
    )
    ''')
    conn.commit()
    
    cursor.execute('''
    INSERT INTO rules (rule_string, ast) VALUES (?, ?)
    ''', (rule_string, ast))
    conn.commit()
    print("Stored in SQLite:", rule_string)
    
    conn.close()

# Function to retrieve from MongoDB
def retrieve_rule_mongo(rule_string):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['rule_engine']
    rules_collection = db['rules']

    stored_rule = rules_collection.find_one({"rule_string": rule_string})
    if stored_rule:
        return json.loads(stored_rule['ast'])
    return None

# Function to retrieve from SQLite
def retrieve_rule_sqlite(rule_string):
    conn = sqlite3.connect('rules.db')
    cursor = conn.cursor()
    
    cursor.execute('SELECT ast FROM rules WHERE rule_string = ?', (rule_string,))
    stored_ast = cursor.fetchone()
    
    conn.close()
    
    if stored_ast:
        return json.loads(stored_ast[0])
    return None

# Main execution
if __name__ == "__main__":
    # Define a sample rule
    rule_string = "((age > 30 AND department = 'Sales') OR (age < 25 AND department = 'Marketing')) AND (salary > 50000 OR experience > 5)"
    
    # Create AST
    ast = create_rule(rule_string)
    serialized_ast = json.dumps(serialize_ast(ast))

    # Store in MongoDB
    store_rule_mongo(rule_string, serialized_ast)

    # Store in SQLite
    store_rule_sqlite(rule_string, serialized_ast)

    # Retrieve and evaluate from MongoDB
    retrieved_ast_mongo = retrieve_rule_mongo(rule_string)
    if retrieved_ast_mongo:
        retrieved_ast_mongo = deserialize_ast(retrieved_ast_mongo)
    user_data = {"age": 35, "department": "Sales", "salary": 60000, "experience": 3}
    result_mongo = evaluate_rule(retrieved_ast_mongo, user_data)
    print("MongoDB Evaluation Result:", result_mongo)

    # Retrieve and evaluate from SQLite
    retrieved_ast_sqlite = retrieve_rule_sqlite(rule_string)
    if retrieved_ast_sqlite:
        retrieved_ast_sqlite = deserialize_ast(retrieved_ast_sqlite)
    result_sqlite = evaluate_rule(retrieved_ast_sqlite, user_data)
    print("SQLite Evaluation Result:", result_sqlite)
