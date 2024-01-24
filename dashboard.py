from dash import Dash, html, dcc
from dash.dash_table import DataTable
import plotly.express as px
import duckdb

app = Dash(__name__)
conn = duckdb.connect('database/transactions.db')
sum_price_per_store_query = 'SELECT store, SUM(price) AS total_price FROM df_transactions GROUP BY store'
sum_price_per_store = conn.execute(sum_price_per_store_query).fetch_df()
top_3_vendas = conn.execute('''SELECT store, SUM(price) AS total_price FROM df_transactions
                            GROUP BY store ORDER BY total_price DESC LIMIT 3''').fetch_df()
top_3_vendas['total_price']= top_3_vendas['total_price'].round(2)
conn.close()

# criando o gráfico
fig1 = px.bar(sum_price_per_store)
fig1.update_layout(
    xaxis_title='loja',  # Set the x-axis label to 'loja'
    yaxis_title='total de Vendas em R$',  # Set the y-axis label to 'total de Vendas em R$'
)

app.layout = html.Div(children=[
    html.H1(children='Total de vendas por loja'),
    #html.H2(children='Gráfico com o Faturamento de Todos os Produtos separados por Loja'),
    #html.Div(children='''
    #    Obs: Esse gráfico mostra a quantidade de produtos vendidos, não o faturamento.
    #'''),
#
    #dcc.Dropdown(opcoes, value='Todas as Lojas', id='lista_lojas'),

    dcc.Graph(
        id='grafico_quantidade_vendas',
        figure=fig1
    ),
    html.H3(children='Top 3 lojas que mais venderam e suas vendas totais'),
    DataTable(
        id='top_3_vendas',
        columns=[
            {'name': 'loja', 'id': 'store'},  # Change 'store' to 'loja'
            {'name': 'total de vendas em R$', 'id': 'total_price'}  # Change 'price' to 'total de vendas em R$'
        ],
        data=top_3_vendas.to_dict('records'),
        style_table={'width': '50%'}
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