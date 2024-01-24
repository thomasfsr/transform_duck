from dash import Dash, html, dcc
from dash.dash_table import DataTable
import plotly.express as px
import duckdb

app = Dash(__name__)
conn = duckdb.connect('database/transactions.db')
sum_price_per_store_query = 'SELECT store AS Loja, SUM(price) AS "total de Vendas (R$)" FROM df_transactions GROUP BY store'
sum_price_per_store = conn.execute(sum_price_per_store_query).fetch_df()
top_3_vendas = conn.execute('''SELECT store AS Loja, SUM(price) AS "Total de Vendas (R$)" FROM df_transactions
                            GROUP BY Loja ORDER BY "Total de Vendas (R$)" DESC LIMIT 3''').fetch_df()
top_3_vendas['Total de Vendas (R$)']= top_3_vendas['Total de Vendas (R$)'].round(2)

timeline_vendas_query = '''SELECT EXTRACT(DAY FROM transaction_time) AS Dia, 
                            SUM(price) AS "Total de Vendas (R$)"
                            FROM df_transactions 
                            GROUP BY Dia
                            '''
timeline_vendas = conn.execute(timeline_vendas_query).fetch_df()
conn.close()

fig1 = px.bar(sum_price_per_store["total de Vendas (R$)"])
fig1.update_layout(
    xaxis_title='loja',  # Set the x-axis label to 'loja'
    yaxis_title='total de Vendas em R$',  # Set the y-axis label to 'total de Vendas em R$'
    xaxis=dict(title=dict(font=dict(size=16))),  # Increase the size of the x-axis title
    yaxis=dict(title=dict(font=dict(size=16))) 
)

fig2 = px.line(timeline_vendas, x='Dia', y='Total de Vendas (R$)'
               #title='Total de Vendas por Dia'
               )
fig2.update_layout(xaxis_title='Dia', yaxis_title='Total de Vendas (R$)',
                   xaxis=dict(title=dict(font=dict(size=16))),
                   yaxis=dict(title=dict(font=dict(size=16))))

app.layout = html.Div(children=[
    html.H1(children='Dados da tabela de transações',
            style={'fontFamily': 'Arial, sans-serif', 'color': 'blue','textAlign': 'center'}),
    #html.H2(children='Gráfico com o Faturamento de Todos os Produtos separados por Loja'),
    #html.Div(children='''
    #    Obs: Esse gráfico mostra a quantidade de produtos vendidos, não o faturamento.
    #'''),
#
    #dcc.Dropdown(opcoes, value='Todas as Lojas', id='lista_lojas'),
    html.P(children='Gráfico de total de vendas por loja', style={'textAlign': 'center','fontFamily': 'Arial', 'color': '#3e3f3f','font-size': '22px'}), 
    dcc.Graph(
        id='grafico_quantidade_vendas',
        figure=fig1
    ),
    html.P(children='Top 3 lojas que mais venderam', style={'fontFamily': 'Arial', 'color': '#3e3f3f','font-size': '22px','textAlign': 'center'}),
    DataTable(
        id='top_3_vendas',
        columns=[
            {'name': 'Loja', 'id': 'Loja'},  # Change 'store' to 'loja'
            {'name': 'Total de Vendas (R$)', 'id': 'Total de Vendas (R$)'}  # Change 'price' to 'total de vendas em R$'
        ],
        data=top_3_vendas.to_dict('records'),
        style_table={'width': '50%', 'margin': 'auto' },
        style_cell={'textAlign': 'center', 'fontSize': 18},
        style_header={
        'backgroundColor': '#3e3f3f',
        'fontWeight': 'bold',
        'color':'white'},
        style_data={
        'backgroundColor': 'rgb(200, 200, 200)',
        'color': '#3e3f3f'},
    ),
    html.P(children='Total de Vendas por dia', style={'textAlign': 'center','fontFamily': 'Arial', 'color': '#3e3f3f','font-size': '22px'}),
    dcc.Graph(
        id='line-plot',
        figure=fig2
    )
])

#@app.callback(
#    Output('grafico_quantidade_vendas', 'figure'),
#    Input('lista_lojas', 'value')
#)
#def update_output(value):
#    if value == "Todas as Lojas":
#        fig = px.bar(df, x="Produto", y="Quantidade", color="ID Loja", barmode="group")
#    else:
#        tabela_filtrada = df.loc[df['ID Loja']==value, :]
#        fig = px.bar(tabela_filtrada, x="Produto", y="Quantidade", color="ID Loja", barmode="group")
#    return fig


if __name__ == '__main__':
    app.run_server(debug=True)