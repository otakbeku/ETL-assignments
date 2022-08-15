from distutils.sysconfig import customize_compiler
from flask import Flask, jsonify, request
from runner import csv_to_sqlite

# Dash
from dash import Dash
import dash_html_components as html


server = Flask(__name__)
app = Dash(
    __name__,
    server=server,
    url_base_pathname='/dash/'
)

app.layout = html.Div(id='dash-container')

@server.route('/etl', method=['POST'])
def etl_function():
    customer_path = "csv_sample_data/Customer_ID_Superstore.csv"
    order_path = "csv_sample_data/final_superstore.csv"
    product_path = "csv_sample_data/Product_ID_Superstore.csv"
    db_path = 'webserver.db'
    csv_to_sqlite(customer_path, 'Customer_Superstore', db_path)
    csv_to_sqlite(order_path, 'Order_Superstore', db_path)
    csv_to_sqlite(product_path, 'Product_Superstore', db_path)
    return jsonify(str("ETL operation successfully executed" ))

@server.route("/viz", methods=["GET"])
def message():
    posted_data = request.get_json()
    name = posted_data['name']
    return jsonify(" Hope you are having a good time " +  name + "!!!")


if __name__=='__main__':
    app.run(debug=True, port=8051)
