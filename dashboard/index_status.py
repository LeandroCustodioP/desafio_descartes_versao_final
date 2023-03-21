from dash import Dash, html, Input, Output, dash_table, dcc
import pandas as pd
import os


# Lendo um arquivo parquet com os dados e crie um dataframe:
caminho = os.path.join("..", "data_lake", "business")
df = pd.read_parquet(os.path.join(caminho, "df_status.parquet"))

descricao = df['STATUS'].unique()

app = Dash(__name__)

app.layout = html.Div(
    [
        html.H4("Status dos Pedidos"),
        html.Div(
            [
                html.Label("Selecione o Tipo do Status:"),
                dcc.Dropdown(
                    id="descricao-dropdown",
                    options=[{"label": estado, "value": estado} for estado in descricao],
                    value=descricao[0],
                    clearable=False,
                ),
            ],
            style={"width": "50%", "display": "inline-block"},
        ),
        html.Div(
            [
                html.Label("Filtrar por Intervalo de Tempo:"),
                dcc.DatePickerRange(
                    id="date-range-picker",
                    start_date=df["CREATION_DATE"].min(),
                    end_date=df["CREATION_DATE"].max(),
                ),
            ],
            style={"width": "50%", "display": "inline-block"},
        ),
        html.P(id="table_out"),
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
    Output("table", "data"),
    Input("descricao-dropdown", "value"),
    Input("date-range-picker", "start_date"),
    Input("date-range-picker", "end_date"),
)
def update_table(selected_estado, start_date, end_date):
    filtered_df = df[
        (df["STATUS"] == selected_estado)
        & (df["CREATION_DATE"] >= start_date)
        & (df["CREATION_DATE"] <= end_date)
    ]
    return filtered_df.to_dict("records")


@app.callback(Output("table_out", "children"), Input("table", "active_cell"))
def update_graphs(active_cell):
    if active_cell:
        cell_data = df.iloc[active_cell["row"]][active_cell["column_id"]]
        return f'Data: "{cell_data}" from table cell: {active_cell}'
    return ""


if __name__ == "__main__":
    app.run_server(port = 8050, debug=True)
