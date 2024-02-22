from flask import Flask, render_template, request, redirect, url_for, session
import pandas as pd
import pymysql
import psycopg2
import pymssql
import pyodbc

app = Flask(__name__, template_folder='templates')
app.secret_key = 'BAD_SECRET_KEY'

# Function to read data from Excel file
def read_excel(file_path):
    df = pd.read_excel(file_path)
    return render_template('excel_data.html', excel_data=df)

# Function to fetch tables from MySQL database
def fetch_mysql_tables( user, password, database):
    connection = pymysql.connect(host='localhost',user=user,password=password,database=database)
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    connection.close()
    return tables

# Function to fetch tables from PostgreSQL database
def fetch_postgresql_tables( user, password, database):
    connection = psycopg2.connect(host='localhost', user=user, password=password, database=database)
    cursor = connection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = [table[0] for table in cursor.fetchall()]
    connection.close()
    return tables

# Function to fetch tables from SQL Server Management Studio (SSMS)
def fetch_ssms_tables(server_name, database):
    try:
        connection = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                                    "Server="+server_name+";"
                                    "Trusted_Connection=yes;")
        cursor = connection.cursor()
        cursor.execute('use '+database)
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
        table_names = cursor.fetchall()
        tables = [table[0] for table in table_names]
        connection.close()
        return tables
    except pymssql.Error as e:
        # Handle connection error
        print("Error connecting to SSMS:", e)
        return []

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/fetch-data', methods=['POST'])
def fetch_data():
    data_source = request.form['data-source']
    if data_source == 'excel':
        uploaded_file = request.files['file']
        if uploaded_file.filename != '':
            file_path = 'uploads/' + uploaded_file.filename
            uploaded_file.save(file_path)
            data = read_excel(file_path)
            return data
        else:
            return 'No file selected'
    elif data_source in ['mysql', 'postgresql', 'ssms']:
        return redirect(url_for('database_input', data_source=data_source))
    else:
        return 'Invalid data source'

@app.route('/database-input/<data_source>', methods=['GET', 'POST'])
def database_input(data_source):
    if request.method == 'GET':
        if data_source == 'ssms':
            return render_template('ssms_input.html')
        else:
            return render_template('database_input.html', data_source=data_source)
    else:
        if data_source == 'ssms':
            return redirect(url_for('ssms_input'))
        else:
            user = request.form['user']
            database = request.form['database']
            if user is not None and database is not None:
                session['servername'] = user
                session['database'] = database
            tables = []  # Initialize tables here
            if data_source == 'mysql':
                mysql_user=request.form['user']
                mysql_pass=request.form['password']
                mysql_database=request.form['database']
                session['mysql_user']=mysql_user
                session['mysql_pass']=mysql_pass
                session['mysql_database']=mysql_database
                tables = fetch_mysql_tables(mysql_user, mysql_pass,mysql_database)
                return render_template('mysql_select_table.html', tables=tables)
            elif data_source == 'postgresql':
                post_user=request.form['user']
                post_pass=request.form['password']
                post_database=request.form['database']
                session['post_user']=post_user
                session['post_pass']=post_pass
                session['post_database']=post_database
                tables = fetch_postgresql_tables(post_user, post_pass,post_database)
                return render_template('post_select_table.html', tables=tables)

@app.route('/ssms-input', methods=['GET', 'POST'])
def ssms_input():
    if request.method == 'GET':
        return render_template('ssms_input.html')
    else:
        server_name = request.form['server_name']
        database = request.form['database']
        if server_name is not None and database is not None:
            session['servername'] = server_name
            session['database'] = database
        tables = fetch_ssms_tables(server_name, database)
        print("Fetched tables:", tables)  # Debugging statement
        return render_template('select_table.html', tables=tables)

@app.route('/database-display-data', methods=['GET', 'POST'])
def ssms_display_data():
    if request.method == 'POST':
        table_name = request.form['table']
        try:
            connection = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                                        "Server="+session['servername']+";"
                                        "Trusted_Connection=yes;")
            cursor = connection.cursor()
            cursor.execute('use '+session['database'])
            sql_query = "SELECT * from "+table_name+";"
            df = pd.read_sql_query(sql_query, connection)
            html_table = df.to_html()
            cursor.close()
            connection.close()
            return render_template('display_data.html', html_table=html_table)
        except Exception as e:
            return str(e)
@app.route('/postgres-display-data', methods=['GET', 'POST'])
def postgres_display_data():
    if request.method == 'POST':
        table_name = request.form['table']
        try:
            connection = psycopg2.connect(host='localhost', user=session['post_user'], password=session['post_pass'], database=session['post_database'])
            cursor = connection.cursor()
            sql_query = "SELECT * from "+table_name+";"
            df = pd.read_sql_query(sql_query, connection)
            html_table = df.to_html()
            cursor.close()
            connection.close()
            return render_template('display_data.html', html_table=html_table)
        except Exception as e:
            return str(e)

@app.route('/mysql-display-data', methods=['GET', 'POST'])
def mysql_display_data():
    if request.method == 'POST':
        table_name = request.form['table']
        try:
            connection = pymysql.connect(host='localhost',user=session['mysql_user'],password=session['mysql_pass'],database=session['mysql_database'])
            cursor = connection.cursor()
            sql_query = "SELECT * from "+table_name+";"
            df = pd.read_sql_query(sql_query, connection)
            html_table = df.to_html()
            cursor.close()
            connection.close()
            return render_template('display_data.html', html_table=html_table)
        except Exception as e:
            return str(e)
            
            

if __name__ == '__main__':
    app.run(debug=True)
