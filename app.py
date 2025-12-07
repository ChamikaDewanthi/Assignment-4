from flask import Flask, render_template, request, jsonify
from db_logic import get_tables, perform_operation
import json

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_tables', methods=['POST'])
def get_tables_route():
    data = request.get_json()
    tables = get_tables()
    return jsonify({'tables': tables})

@app.route('/execute', methods=['POST'])
def execute():
    data = request.get_json()
    table = data.get('table')
    operation = data.get('operation')
    query = data.get('query', '')
    
    result = perform_operation(table, operation, query)    
    json_result = json.dumps({'result': result}, indent=4, ensure_ascii=False)
    return json_result

if __name__ == '__main__':
    app.run(debug=True)