import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
import MySQLdb
from io_modules import get_latestfile

feature_options = ["HasDetections","SmartScreen","AVProductsInstalled",
                   "CountryIdentifier", "AVProductStatesIdentifier",
                   "Census_OSVersion", "EngineVersion",
                   "AppVersion", "Census_OSBuildRevision",
                   "GeoNameIdentifier", "OsBuildLab"]

app = dash.Dash()

app.layout = html.Div([
    html.H2("Model Quality Assessment with Top 10 Features"),
    html.Div(
        [
            dcc.Dropdown(
                id="features",
                options=[{
                    'label': i,
                    'value': i
                } for i in feature_options],
                value="SmartScreen"),
        ],
        style={'width': '25%',
               'display': 'inline-block'}),
    dcc.Graph(id='funnel-graph'),
])


@app.callback(
    dash.dependencies.Output('funnel-graph', 'figure'),
    [dash.dependencies.Input('features', 'value')])
def update_graph(Feature):

    def table_query(Feature,isTrain=True):
        if isTrain == True:
            dbName = "Training"
        else:
            dbName = "Prediction"

        conn = MySQLdb.connect(host="ec2-34-211-3-37.us-west-2.compute.amazonaws.com",
                               user="USERNAME", passwd="PASSWORD", db=dbName)
        cursor = conn.cursor()
        get_table = get_latestfile("")
        cursor.execute("select COUNT({}) FROM {} GROUPBY {}".format(Feature, get_table, Feature));
        plot_data = dict(cursor.fetchall())
        cursor.execute("select COUNT({}) FROM {}".format(Feature, get_table));
        count_data = dict(cursor.fetchall())
        n_data = count_data(Feature)
        return plot_data, n_data

    df_plot_train, n_train = table_query(Feature,True)
    df_plot_test, n_test = table_query(Feature,False)

    data_train = {'x': df_plot_train.iloc[:,0], 'y': df_plot_train.iloc[:,1]/n_train, 'type': 'bar', 'name': 'Training Data Distribution'}
    data_test = {'x': df_plot_test.iloc[:,0], 'y': df_plot_test.iloc[:,1]/n_test, 'type': 'bar', 'name': 'Preiction Data Distribution'}

    return {
        'data': [data_train, data_test],
        'layout':
        go.Layout(
            if Feature == "HasDetections":
                title ="Distribution of Detections for Training and Prediction Data"
            else:
                title="Feature Distribution for {}".format(Feature))
    }


if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
