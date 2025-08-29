%md
# Pipeline de Ingestão – YellowTripDataIngestion

Esta pasta contém todo o código-fonte do pipeline **'YellowTripDataIngestion'**:

- `explorations`: notebooks ad-hoc utilizados para explorar os dados processados por este pipeline.  
- `transformations`: todas as definições de datasets e transformações aplicadas.  
- `utilities`: funções utilitárias e módulos Python utilizados neste pipeline.  

%md
## Transformações das Camadas Bronze, Silver e Gold

O pipeline **YellowTripDataIngestion** aplica uma série de transformações organizadas em três camadas principais, garantindo qualidade, consistência e agregação dos dados para análises avançadas:

### Bronze → Silver
- **Limpeza e filtragem de dados brutos**: remoção de registros com valores ausentes críticos, distâncias de viagem negativas, horários inconsistentes ou contagem de passageiros inválida.  
- **Validação de esquema**: conferência dos tipos de dados conforme o dicionário oficial da TLC.  
- **Verificação de consistência financeira**: checagem de `total_amount` em relação à soma de tarifas, pedágios, gorjetas e outros encargos, com tolerância de até 3%.  
- **Detecção e remoção de duplicados**: garantindo que apenas registros únicos avancem para a próxima camada.  
- **Normalização e padronização**: conversão de códigos categóricos, ajuste de tipos numéricos e preenchimento de valores padrão quando necessário.

### Silver → Gold
- **Agregações temporais e geográficas**: cálculo de métricas por local de embarque, dia da semana, ano e mês.  
- **Cálculo de métricas de viagem**: total de viagens, receita, tarifas, gorjetas, pedágios, distância média, tempo médio de viagem e percentual de gorjeta.  
- **Criação de faixas de distância**: categorização das viagens em curtas, médias e longas, permitindo análise de padrões de viagem e impacto na receita.  
- **Agregações por características de passageiros**: métricas por contagem de passageiros, dia da semana e hora de embarque.  
- **Padronização de saída**: arredondamento de métricas e formatação consistente para dashboards e relatórios, garantindo uniformidade entre todas as tabelas Gold.


