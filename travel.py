import dash
from dash import Dash, html, dcc, Input, Output, State, no_update, ctx, callback
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import dash_ag_grid as grid
import numpy as np
import io
from flask import Flask
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy as sa
from google.cloud.sql.connector import Connector
import urllib
import dash_auth

import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "application_default_credentials.json"

columnDefs_natalia = [
    {
        "headerName": "#",  # Name of table displayed in app
        "field": "entry",       # ID of table (needs to be the same as SQL column name)
        "type":"numericColumn",
        "editable":False,
        # "rowDrag": False,         # only need to activate on the first row for all to be draggable
        "checkboxSelection": True,  # only need to activate on the first row
        # "hide":"True",
        "floatingFilter": False
    },
    {
        "headerName": "Countries",
        "field": "country",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True",
        "checkboxSelection": False
    },
    {
        "headerName": "City",
        "field": "city",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "City Latitude",
        "field": "city_lat",
        "type": "numericColumn",
        # "editable":False,
        "filter": "agNumberColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "City Longitude",
        "field": "city_lon",
        "type": "numericColumn",
        # "editable":False,
        "filter": "agNumberColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "Countries (PL)",
        "field": "country_pl",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True",
        "checkboxSelection": False
    },
    {
        "headerName": "City (PL)",
        "field": "city_pl",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "Concatenate",
        "field": "concatenate",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        "hide":"True"
    }
]

columnDefs_inlaws = [
    {
        "headerName": "#",  # Name of table displayed in app
        "field": "entry",       # ID of table (needs to be the same as SQL column name)
        "type":"numericColumn",
        "editable":False,
        # "rowDrag": False,         # only need to activate on the first row for all to be draggable
        "checkboxSelection": True,  # only need to activate on the first row
        # "hide":"True",
        "floatingFilter": False
    },
    {
        "headerName": "Countries",
        "field": "country",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True",
        "checkboxSelection": False
    },
    {
        "headerName": "City",
        "field": "city",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "City Latitude",
        "field": "city_lat",
        "type": "numericColumn",
        # "editable":False,
        "filter": "agNumberColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "City Longitude",
        "field": "city_lon",
        "type": "numericColumn",
        # "editable":False,
        "filter": "agNumberColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "Countries (PL)",
        "field": "country_pl",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True",
        "checkboxSelection": False
    },
    {
        "headerName": "City (PL)",
        "field": "city_pl",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "Concatenate",
        "field": "concatenate",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        "hide":"True"
    }
]

columnDefs_visited = [
    {
        "headerName": "#",  # Name of table displayed in app
        "field": "entry",       # ID of table (needs to be the same as SQL column name)
        "type":"numericColumn",
        "editable":False,
        # "rowDrag": False,         # only need to activate on the first row for all to be draggable
        "checkboxSelection": True,  # only need to activate on the first row
        # "hide":"True",
        "floatingFilter": False
    },
    {
        "headerName": "Countries",
        "field": "country",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True",
        "checkboxSelection": False
    },
    {
        "headerName": "City",
        "field": "city",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "City Latitude",
        "field": "city_lat",
        "type": "numericColumn",
        # "editable":False,
        "filter": "agNumberColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "City Longitude",
        "field": "city_lon",
        "type": "numericColumn",
        # "editable":False,
        "filter": "agNumberColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "Countries (PL)",
        "field": "country_pl",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True",
        "checkboxSelection": False
    },
    {
        "headerName": "City (PL)",
        "field": "city_pl",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "Concatenate",
        "field": "concatenate",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        "hide":"True"
    }
]

columnDefs_countries = [
    {
        "headerName": "#",  # Name of table displayed in app
        "field": "entry",       # ID of table (needs to be the same as SQL column name)
        "type":"numericColumn",
        "editable":False,
        # "rowDrag": False,         # only need to activate on the first row for all to be draggable
        "checkboxSelection": True,  # only need to activate on the first row
        # "hide":"True",
        "floatingFilter": False
    },
    {
        "headerName": "Countries",
        "field": "country",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True",
        "checkboxSelection": False
    },
    {
        "headerName": "Alpha-2",
        "field": "alpha_2",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True",
    },
    {
        "headerName": "Alpha-3",
        "field": "alpha_3",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True",
    },
    {
        "headerName": "Numeric",
        "field": "Numeric",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "numericColumn",
        # "hide":"True",
        # "floatingFilter": False
    },
    {
        "headerName": "Elmar",
        "field": "traveled",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True",
        # "floatingFilter": False,
        "cellEditor":"agSelectCellEditor",
        "cellEditorParams":{"values":[
                                        "yes",
                                        "",
                                    ]}
    },
    {
        "headerName": "Natalia",
        "field": "traveled_natalia",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True",
        # "floatingFilter": False,
        "cellEditor":"agSelectCellEditor",
        "cellEditorParams":{"values":[
                                        "yes",
                                        "",
                                    ]}
    },
    {
        "headerName": "Inlaws",
        "field": "traveled_inlaws",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True",
        # "floatingFilter": False,
        "cellEditor":"agSelectCellEditor",
        "cellEditorParams":{"values":[
                                        "yes",
                                        "",
                                    ]}
    },
    {
        "headerName": "Capital",
        "field": "capital",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "Capital Latitude",
        "field": "capital_lat",
        "type": "numericColumn",
        # "editable":False,
        "filter": "agNumberColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "Capital Longitude",
        "field": "capital_lon",
        "type": "numericColumn",
        # "editable":False,
        "filter": "agNumberColumnFilter",
        # "hide":"True"
    },
    {
        "headerName": "Concatenate",
        "field": "concatenate",
        # "type": "rightAligned",
        # "editable":False,
        "filter": "agTextColumnFilter",
        "hide":"True"
    }
]


driver_name="SQL Server"
server_name="34.118.49.180"
database_name="planner" 

params = urllib.parse.quote_plus("DRIVER={SQL Server};"
                                 "SERVER="+server_name+";"
                                 "DATABASE="+database_name+";"
                                 "trusted_connection=yes")

# engine = sa.create_engine("mssql+pyodbc://sqlserver:PlannerPW123!/?odbc_connect={}".format(params))

# initialize Connector object
connector = Connector()

user = "sqlserver"
password = "PlannerPW123!"
host = "34.118.49.180"
port = 1433
database = "planner"
instance = "arctic-crawler-410012:europe-central2:planner-data" #"project_id:region:instance_name"
driver="ODBC Driver 18 for SQL Server"

def get_con():
    conn = connector.connect(
        instance,
        "pytds",
        user=user,
        password=password,
        db=database
    )
    return conn


# engine = sa.create_engine(f"mysql+pyodbc://{0}:{1}@{2}{3}/{4}?driver={5}".format(user, password, host, instance, database, driver))

engine = sa.create_engine(
    "mssql+pytds://",
    creator=get_con,
    )

con=engine.connect()
# df=pd.read_sql_table("planner",con)
# print(df.head())
# query="select * from dbo.Your_Sales_Data$"

server = Flask(__name__)
app = dash.Dash(__name__, server=server, suppress_callback_exceptions=True, external_stylesheets=[dbc.themes.SUPERHERO])
app.server.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# for your home SQL test table
app.server.config["SQLALCHEMY_DATABASE_URI"] = "mssql+pyodbc:///?odbc_connect={}".format(params)

auth = dash_auth.BasicAuth(
        app,
        {
            "guest":"guest"
        }
    )

db = SQLAlchemy(app.server)

def sql_countries():
    update_concat_command=sa.text("IF COL_LENGTH('countries', 'concatenate') IS NOT NULL BEGIN ALTER TABLE countries drop column concatenate END")
    update_concat_sql = con.execute(update_concat_command)
    update_concat_command_1=sa.text("ALTER TABLE countries add concatenate varchar(MAX)")
    update_concat_sql_1 = con.execute(update_concat_command_1)
    update_concat_command_2=sa.text("UPDATE countries set concatenate=CONCAT(entry, alpha_2, alpha_3, numeric, traveled, capital, capital_lat, capital_lon)")
    update_concat_sql_2 = con.execute(update_concat_command_2)
    df_countries = pd.read_sql_table("countries", con=con)
    return df_countries

def sql_visited():
    update_concat_command=sa.text("IF COL_LENGTH('visited', 'concatenate') IS NOT NULL BEGIN ALTER TABLE visited drop column concatenate END")
    update_concat_sql = con.execute(update_concat_command)
    update_concat_command_1=sa.text("ALTER TABLE visited add concatenate varchar(MAX)")
    update_concat_sql_1 = con.execute(update_concat_command_1)
    update_concat_command_2=sa.text("UPDATE visited set concatenate=CONCAT(entry, country, city, city_lat, city_lon)")
    update_concat_sql_2 = con.execute(update_concat_command_2)
    df_visited = pd.read_sql_table("visited", con=con)
    return df_visited

def sql_visited_natalia():
    update_concat_command=sa.text("IF COL_LENGTH('visited_natalia', 'concatenate') IS NOT NULL BEGIN ALTER TABLE visited_natalia drop column concatenate END")
    update_concat_sql = con.execute(update_concat_command)
    update_concat_command_1=sa.text("ALTER TABLE visited_natalia add concatenate varchar(MAX)")
    update_concat_sql_1 = con.execute(update_concat_command_1)
    update_concat_command_2=sa.text("UPDATE visited_natalia set concatenate=CONCAT(entry, country, city, city_lat, city_lon)")
    update_concat_sql_2 = con.execute(update_concat_command_2)
    df_visited_natalia = pd.read_sql_table("visited_natalia", con=con)
    return df_visited_natalia

def sql_visited_inlaws():
    update_concat_command=sa.text("IF COL_LENGTH('visited_inlaws', 'concatenate') IS NOT NULL BEGIN ALTER TABLE visited_inlaws drop column concatenate END")
    update_concat_sql = con.execute(update_concat_command)
    update_concat_command_1=sa.text("ALTER TABLE visited_inlaws add concatenate varchar(MAX)")
    update_concat_sql_1 = con.execute(update_concat_command_1)
    update_concat_command_2=sa.text("UPDATE visited_inlaws set concatenate=CONCAT(entry, country, city, city_lat, city_lon)")
    update_concat_sql_2 = con.execute(update_concat_command_2)
    df_visited_inlaws = pd.read_sql_table("visited_inlaws", con=con)
    return df_visited_inlaws

app.layout = dbc.Container([
    dbc.Row([
        html.Br(),
        dcc.Interval(id="interval-map", interval=86400000*7, n_intervals=0),
        dcc.Interval(id="interval-map-1", interval=86400000*7, n_intervals=0),
        dcc.Interval(id="interval-visited", interval=86400000*7, n_intervals=0),
        dcc.Interval(id="interval-visited-1", interval=86400000*7, n_intervals=0),
        dcc.Store(id="countries", data=0),
        dcc.Store(id="store-countries", data=0),
        dcc.Store(id="store-countries-1", data=0),
        dcc.Store(id="store-visited", data=0),
        dcc.Store(id="store-visited-1", data=0),
        dcc.Store(id="travel-list", data=0),
        dcc.Store(id="travel-list-iso", data=0),
        dcc.Store(id="selected-countries", data=0),
        dcc.Store(id="selected-visited", data=0),
        dcc.Store(id="capital", data=0),
        dcc.Store(id="visited", data=0)
    ]),
    dbc.Row(html.Br()),
    dbc.Row([
        dbc.Col(
            dcc.Dropdown(id="zoom-country"),
            width=3
        ),
        dbc.Col(
            dcc.RadioItems(
                [" Natural Earth", " Orthographic"],
                " Natural Earth",
                id="projection-type"
            ),
            width=1
        ),
        dbc.Col(
            dcc.RadioItems(
                [" Elmar", " Natalia", " Inlaws"],
                " Elmar",
                id="who-traveled"
            )
        )
    ]),
    dbc.Row(html.Br()),
    dbc.Row(dcc.Graph(id="map", style={"height":"75vh"}), align="center"),
    dbc.Row(html.Br()),
    dbc.Row([
        dbc.Col(html.Div(
                id="table-from-sql-countries",
                style={"height":"75vh"} #"vh" stands for "viewport's height"
            )),
        dbc.Col(html.Div(
                id="table-from-sql-visited",
                style={"height":"75vh"} #"vh" stands for "viewport's height"
            )),
    dbc.Row([
        dbc.Col([
            html.Span(
                [
                    dbc.Button(id="add-row-button-countries", children="Add row", color="primary", size="md", className="mt-3"),
                    dbc.Button(id="delete-row-button-countries", children="Delete row", color="secondary", size="md", className="mt-3 me-1"),
                    dbc.Button(id="save-to-sql-countries", children="Save to SQL", color="secondary", size="md", className="mt-3 me-1"),
                    dbc.Button(id="save-to-excel-countries", children="Save to Excel", color="secondary", size="md", className="mt-3 me-1")
                ]
            ),
            html.Div(id="placeholder-countries", children=[]),
            html.Div(id="placeholder-countries-1", children=[]),
        ], width=6),
        dbc.Col([
            html.Span(
                [
                    dbc.Button(id="add-row-button-visited", children="Add row", color="primary", size="md", className="mt-3"),
                    dbc.Button(id="delete-row-button-visited", children="Delete row", color="secondary", size="md", className="mt-3 me-1"),
                    dbc.Button(id="save-to-sql-visited", children="Save to SQL", color="secondary", size="md", className="mt-3 me-1"),
                    dbc.Button(id="save-to-excel-visited", children="Save to Excel", color="secondary", size="md", className="mt-3 me-1")
                ]
            ),
            html.Div(id="placeholder-visited", children=[]),
            html.Div(id="placeholder-visited-1", children=[]),
        ], width=6)
    ], align="center"),
    ])
], fluid=True)

# ------------------------------------------------------------------------------------------------

#Get data from SQL

@callback(
            Output("zoom-country", "options"),
            Output("travel-list", "data"),
            Output("travel-list-iso", "data"),
            Output("countries", "data"),
            Output("capital", "data"),
            Output("visited", "data"),
            Input("interval-map", "n_intervals"),
            Input("who-traveled", "value"),
)

def call_sql_countries(n, person):
    df_countries=pd.DataFrame(sql_countries())
    if person==" Elmar":
        df_visited=pd.DataFrame(sql_visited())
        traveled_iso = df_countries.query("traveled=='yes'")["alpha_3"]
        traveled = df_countries.query("traveled=='yes'")["country"]
        capital = df_countries.query("traveled=='yes'")["capital"]
    elif person==" Natalia":
        df_visited=pd.DataFrame(sql_visited_natalia())
        traveled_iso = df_countries.query("traveled_natalia=='yes'")["alpha_3"]
        traveled = df_countries.query("traveled_natalia=='yes'")["country"]
        capital = df_countries.query("traveled_natalia=='yes'")["capital"]
    else:
        df_visited=pd.DataFrame(sql_visited_inlaws())
        traveled_iso = df_countries.query("traveled_inlaws=='yes'")["alpha_3"]
        traveled = df_countries.query("traveled_inlaws=='yes'")["country"]
        capital = df_countries.query("traveled_inlaws=='yes'")["capital"]
    return traveled, traveled, traveled_iso, df_countries.to_json(date_format="iso", orient="split"), capital, df_visited.to_json(date_format="iso", orient="split")

#Transfer filtered SQL data to Table/AG Grid

@callback(
        Output("table-from-sql-countries", "children"),
        Output("table-from-sql-visited", "children"),
        Input("countries", "data"),
        Input("visited", "data")
)

def populate_datatable(countries, visited):
    df_countries = pd.read_json(io.StringIO(countries), orient="split")
    df_visited = pd.read_json(io.StringIO(visited), orient="split")
    table_countries = grid.AgGrid(
            id="sql-table-countries",
            style={"height": "100%", "width": "100%"},
            rowData=df_countries.to_dict("records"), 
            columnDefs= columnDefs_countries,
            defaultColDef={
                "resizable": True,
                "sortable": True,
                "filter": True,
                "floatingFilter": True,
                "editable": True,
                "minWidth":125
            },
            columnSize="sizeToFit",
            dashGridOptions={
                "undoRedoCellEditing": True,
                "rowDragManaged": True,
                "animateRows": True,
                "rowDragMultiRow": True,
                "suppressRowClickSelection" : True,
                "rowSelection": "multiple",
                "rowDragEntireRow": True
            }
        )
    table_visited = grid.AgGrid(
            id="sql-table-visited",
            style={"height": "100%", "width": "100%"},
            rowData=df_visited.to_dict("records"), 
            columnDefs= columnDefs_visited,
            defaultColDef={
                "resizable": True,
                "sortable": True,
                "filter": True,
                "floatingFilter": True,
                "editable": True,
                "minWidth":125
            },
            columnSize="sizeToFit",
            dashGridOptions={
                "undoRedoCellEditing": True,
                "rowDragManaged": True,
                "animateRows": True,
                "rowDragMultiRow": True,
                "suppressRowClickSelection" : True,
                "rowSelection": "multiple",
                "rowDragEntireRow": True
            }
        )
    return table_countries, table_visited

#Display the map with need cities and countries

@callback(
    Output("map", "figure"),
    Input("interval-map", "n_intervals"),
    Input("zoom-country", "value"),
    Input("travel-list", "data"),
    Input("travel-list-iso", "data"),
    Input("countries", "data"),
    Input("visited", "data"),
    Input("projection-type", "value"),
    Input("who-traveled", "value")
)

#projection_type="orthographic"

def update_map(n, dropdown, list, list_iso, data, visited, projection, person):
    df = pd.read_json(io.StringIO(data), orient="split")
    df_visited = pd.read_json(io.StringIO(visited), orient="split")
    if dropdown == None:
        fig = px.choropleth(
        list_iso,
        locations=list_iso,
        hover_name=list, # column to add to hover information
        )

        fig.update_layout(
            showlegend=False
        )

        fig.add_trace(go.Scattergeo(
            lat=df_visited["city_lat"],
            lon=df_visited["city_lon"],
            text=df_visited["city"]+", "+df_visited["country"],
            mode = "markers",
            marker = dict(
                color = "#FF0000"
            ),
        ))

        fig.update_geos(
            projection_type=f"{projection.lstrip().lower()}",
            showcountries=True,
            showland=True,
            landcolor="LightGreen",
            showocean=True,
            oceancolor="LightBlue",
            showlakes=True,
            lakecolor="LightBlue",
            showrivers=True,
            rivercolor="LightBlue",
            bgcolor="rgba(0,0,0,0)"
        )
        fig.update_layout( 
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)"
        )
    else:
        if person==" Elmar":
            traveled_iso = df.query(f"traveled=='yes'&country=='{dropdown}'")["alpha_3"]
            traveled = df.query(f"traveled=='yes'&country=='{dropdown}'")["country"]
            capital = df.query(f"traveled=='yes'&country=='{dropdown}'")["capital"]
        elif person==" Natalia":
            traveled_iso = df.query(f"traveled_natalia=='yes'&country=='{dropdown}'")["alpha_3"]
            traveled = df.query(f"traveled_natalia=='yes'&country=='{dropdown}'")["country"]
            capital = df.query(f"traveled_natalia=='yes'&country=='{dropdown}'")["capital"]
        else:
            traveled_iso = df.query(f"traveled_inlaws=='yes'&country=='{dropdown}'")["alpha_3"]
            traveled = df.query(f"traveled_inlaws=='yes'&country=='{dropdown}'")["country"]
            capital = df.query(f"traveled_inlaws=='yes'&country=='{dropdown}'")["capital"]
        fig = px.choropleth(
        traveled_iso,
        locations=traveled_iso,
        hover_name=traveled, # column to add to hover information
        )

        fig.update_layout(
            showlegend=False
        )

        fig.add_trace(go.Scattergeo(
            lat=df_visited.query(f"country=='{dropdown}'")["city_lat"],
            lon=df_visited.query(f"country=='{dropdown}'")["city_lon"],
            text=df_visited.query(f"country=='{dropdown}'")["city_pl"],
            mode = "markers",
            marker = dict(
                color = "#FF0000"
            ),
        ))

        fig.update_geos(
            projection_type=f"{projection.lstrip().lower()}",
            showcountries=True,
            showland=True,
            landcolor="LightGreen",
            showocean=True,
            oceancolor="LightBlue",
            showlakes=True,
            lakecolor="LightBlue",
            showrivers=True,
            rivercolor="LightBlue",
            bgcolor="rgba(0,0,0,0)"
        )

        fig.update_layout(
            autosize=True,
            height=600,
            geo=dict(
                center=dict(
                    lat=df.query(f"country=='{dropdown}'")["capital_lat"].item(),
                    lon=df.query(f"country=='{dropdown}'")["capital_lon"].item()
            ),
            projection_scale=6
        ))
        fig.update_layout( 
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )

    return fig

if __name__=="__main__":
    app.run_server(debug=False)