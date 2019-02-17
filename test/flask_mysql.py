import plotly
plotly.__version__

import plotly.plotly as py
import plotly.graph_objs as go

import MySQLdb

conn = MySQLdb.connect(host="MYSQL_ADRESS",
        user="USERNAME", passwd="PASSWORD", db="Prediction")
cursor = conn.cursor()
cursor.execute("select * FROM R");

rows = cursor.fetchall()
print(rows)
