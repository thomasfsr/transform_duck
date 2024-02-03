import os
import plotly.express as px
from dash import Dash, dcc, html
from dash.dash_table import DataTable
from client_s3 import query_s3, close_conn
from dotenv import load_dotenv

load_dotenv()
aws_access_key_id = os.getenv('aws_access_key_id')
aws_secret_access_key = os.getenv('aws_secret_access_key')
s3_bucket = 'salesretail'
s3_directory = os.getenv('s3_directory')
table = 'sales_retail'
url_s3 = f"read_parquet('s3://{s3_bucket}/{table}.parquet')"

app = Dash(__name__)

sum_price_per_store_query = f'SELECT store AS Loja, SUM(price) AS "total de Vendas (R$)" FROM {url_s3} GROUP BY store'
sum_price_per_store = query_s3(sum_price_per_store_query)

top_3_vendas_query = f"""SELECT store AS Loja, SUM(price) AS "Total de Vendas (R$)" FROM {url_s3} 
                            GROUP BY Loja ORDER BY "Total de Vendas (R$)" DESC LIMIT 3"""
top_3_vendas = query_s3(top_3_vendas_query)

top_3_vendas['Total de Vendas (R$)'] = top_3_vendas[
    'Total de Vendas (R$)'
].round(2)

timeline_vendas_query = f"""SELECT EXTRACT(DAY FROM transaction_time) AS Dia, 
                            SUM(price) AS "Total de Vendas (R$)"
                            FROM {url_s3}
                            GROUP BY Dia
                            """
timeline_vendas = query_s3(timeline_vendas_query)
close_conn()

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

if __name__ == '__main__':
    app.run_server(debug=True)
server = app.server
