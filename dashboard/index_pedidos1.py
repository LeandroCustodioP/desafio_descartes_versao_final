from dash import Dash, html, Input, Output, dash_table, dcc
import pandas as pd
import os


# Lendo um arquivo parquet com os dados e crie um dataframe:
caminho = os.path.join("..", "data_lake", "business")
df = pd.read_parquet(os.path.join(caminho, "pedidos1.parquet"))

descricao = df['DESCRIPTION'].unique()

app = Dash(__name__)

app.layout = html.Div(
    [
        html.H4("Numero de Pedido por Centro de Distribuição"),
        html.P(id="table_out"),
        html.Label('Selecione o Centro de Distribuição:'),
        dcc.Checklist(
            id='descricao-checklist',
            options=[{'label': estado, 'value': estado} for estado in descricao],
            value=descricao,
            labelStyle={'display': 'block'}
        ),
        dash_table.DataTable(
            id="table",
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict("records"),
            style_cell=dict(textAlign="left"),
            style_header=dict(backgroundColor="paleturquoise"),
            style_data=dict(backgroundColor="lavender"),
        ),
    ]
)

@app.callback(
    Output('table', 'data'),
    Input('descricao-checklist', 'value')
)
def update_table(selected_estados):
    filtered_df = df[df['DESCRIPTION'].isin(selected_estados)]
    return filtered_df.to_dict('records')

@app.callback(
    Output("table_out", "children"), 
    Input("table", "active_cell")
)
def update_graphs(active_cell):
    if active_cell:
        cell_data = df.iloc[active_cell["row"]][active_cell["column_id"]]
        return f'Data: "{cell_data}" from table cell: {active_cell}'
    return ""


if __name__ == "__main__":
    app.run_server(debug=True)
