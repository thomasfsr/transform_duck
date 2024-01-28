import duckdb
import plotly.express as px
from dash import Dash, dcc, html
from dash.dash_table import DataTable

app = Dash(__name__)

path_db = 'db/sales.db'
table_name = 'sales_retail'
conn = duckdb.connect(path_db)

sum_price_per_store_query = f'SELECT store AS Loja, SUM(price) AS "total de Vendas (R$)" FROM {table_name} GROUP BY store'
sum_price_per_store = conn.execute(sum_price_per_store_query).fetch_df()

top_3_vendas_query = f"""SELECT store AS Loja, SUM(price) AS "Total de Vendas (R$)" FROM {table_name} 
                            GROUP BY Loja ORDER BY "Total de Vendas (R$)" DESC LIMIT 3"""
top_3_vendas = conn.execute(top_3_vendas_query).fetch_df()

top_3_vendas['Total de Vendas (R$)'] = top_3_vendas[
    'Total de Vendas (R$)'
].round(2)

timeline_vendas_query = f"""SELECT EXTRACT(DAY FROM transaction_time) AS Dia, 
                            SUM(price) AS "Total de Vendas (R$)"
                            FROM {table_name}
                            GROUP BY Dia
                            """
timeline_vendas = conn.execute(timeline_vendas_query).fetch_df()
conn.close()

fig1 = px.bar(sum_price_per_store['total de Vendas (R$)'])
fig1.update_layout(
    xaxis_title='loja',
    yaxis_title='total de Vendas em R$',
    xaxis=dict(title=dict(font=dict(size=16))),
    yaxis=dict(title=dict(font=dict(size=16))),
)

fig2 = px.line(timeline_vendas, x='Dia', y='Total de Vendas (R$)')
fig2.update_layout(
    xaxis_title='Dia',
    yaxis_title='Total de Vendas (R$)',
    xaxis=dict(title=dict(font=dict(size=16))),
    yaxis=dict(title=dict(font=dict(size=16))),
)

app.layout = html.Div(
    children=[
        html.H1(
            children='Dados da tabela de transações',
            style={
                'fontFamily': 'Arial, sans-serif',
                'color': 'blue',
                'textAlign': 'center',
            },
        ),
        html.P(
            children='Gráfico de total de vendas por loja',
            style={
                'textAlign': 'center',
                'fontFamily': 'Arial',
                'color': '#3e3f3f',
                'font-size': '22px',
            },
        ),
        dcc.Graph(id='grafico_quantidade_vendas', figure=fig1),
        html.P(
            children='Top 3 lojas que mais venderam',
            style={
                'fontFamily': 'Arial',
                'color': '#3e3f3f',
                'font-size': '22px',
                'textAlign': 'center',
            },
        ),
        DataTable(
            id='top_3_vendas',
            columns=[
                {'name': 'Loja', 'id': 'Loja'},
                {
                    'name': 'Total de Vendas (R$)',
                    'id': 'Total de Vendas (R$)',
                },
            ],
            data=top_3_vendas.to_dict('records'),
            style_table={'width': '50%', 'margin': 'auto'},
            style_cell={'textAlign': 'center', 'fontSize': 18},
            style_header={
                'backgroundColor': '#3e3f3f',
                'fontWeight': 'bold',
                'color': 'white',
            },
            style_data={
                'backgroundColor': 'rgb(200, 200, 200)',
                'color': '#3e3f3f',
            },
        ),
        html.P(
            children='Total de Vendas por dia',
            style={
                'textAlign': 'center',
                'fontFamily': 'Arial',
                'color': '#3e3f3f',
                'font-size': '22px',
            },
        ),
        dcc.Graph(id='line-plot', figure=fig2),
    ]
)

server = app.server