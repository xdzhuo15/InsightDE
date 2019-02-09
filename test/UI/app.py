import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd

df_test = pd.read_csv(
    "test_result.csv"
)
df_train = pd.read_csv(
    "test_new.csv"
)
feature_options = ["SmartScreen","AVProductStatesIdentifier",
                                "CountryIdentifier", "AVProductsInstalled",
                                "Census_OSVersion", "EngineVersion",
                                "AppVersion", "Census_OSBuildRevision",
                                "GeoNameIdentifier", "OsBuildLab"]

app = dash.Dash()

app.layout = html.Div([
    html.H2("Model Quality Assessment"),
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
    df_plot_train = df_train.groupby(Feature).size().reset_index(name='counts')
    df_plot_test = df_test.groupby(Feature).size().reset_index(name='counts')

    data_train = {'x': df_plot_train.iloc[:,0], 'y': df_plot_train.iloc[:,1], 'type': 'bar', 'name': 'Training Data Distribution'}
    data_test = {'x': df_plot_test.iloc[:,0], 'y': df_plot_test.iloc[:,1], 'type': 'bar', 'name': 'Preiction Data Distribution'}

    return {
        'data': [data_train, data_test],
        'layout':
        go.Layout(
            title='Customer Order Status for {}'.format(Feature))
    }


if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
