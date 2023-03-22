O projeto foi dividido em 3 pastas.

Uma para armazenar os dados chamada de data_lake
esta pasta está dividida em outras três pastas chamadas raw, trusted, business
a pasta raw é onde são armazenados os dados brutos.
a pasta trusted é onde são armazenados os dados após o processo de limpeza.
A pasta de business é onde são armazenados os dados que serão utilizados no processo de confecção dos dashboards.

A pasta denominada codigos de limpeza armazena todos os códigos utilizados para a transformação dos dados. 
existe um arquivo chamado de funcoes_aux.py onde pus algumas funções criadas por mim de modo a organizar melhor o código.
existe também um outro arquivo chamdo limpeza.py onde realizo de fato a limpeza dos dados.
Por fim, existe o arquivo juncao_df onde faço os merges com o intuito de deixar os arquivos no formatos a serem utilizados para os dashboards

A pasta denominada dashboards contem dois arquivos .py chamdos de:
index_pedidos1 onde gero o primeiro dashboard responsável por mostrar quantos pedidos existem por centro de distribuição.
index_status onde faço o segundo dashboard responsável por mostrar quantos pedidos estão com o status de cancelados ou não.
