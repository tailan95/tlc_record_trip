# Yellow Taxi Trip Data Analysis

Este repositório contém todo o código e documentação relacionados à análise de dados de corridas de táxi amarelo (**Yellow Taxi Trip Data**) da cidade de Nova York, com dados **a partir de janeiro de 2023**.

## Sobre os Dados

Os dados utilizados são provenientes da **TLC (Taxi and Limousine Commission)** e incluem informações detalhadas sobre cada corrida, como:  
- Horários de embarque e desembarque  
- Localização de embarque e desembarque  
- Distância percorrida e tempo da viagem  
- Valores pagos, gorjetas, pedágios e outras cobranças  
- Número de passageiros e códigos categóricos diversos  

Todos os registros foram processados e transformados seguindo um pipeline estruturado em três camadas: **Bronze, Silver e Gold**.

## Pipeline de Ingestão e Transformação

O pipeline de ingestão e transformação dos dados está totalmente implementado, garantindo:  
- Limpeza e validação de dados brutos (Bronze → Silver)  
- Transformações, agregações e métricas prontas para análise (Silver → Gold)  
- Consistência de esquema, remoção de duplicados e verificação financeira  
- Preparação de tabelas padronizadas para dashboards e relatórios

## Dashboard Interativo

Um **dashboard interativo** foi desenvolvido para análise exploratória dos dados, permitindo:  
- Visualização de métricas agregadas por tempo, localização e características dos passageiros  
- Análise de padrões de receita, gorjetas e distância de viagens  
- Comparações semanais e mensais, além de filtros por hora e dia da semana  

Este repositório serve como base para análises de desempenho do serviço de táxi amarelo em Nova York, fornecendo tanto os dados limpos quanto ferramentas para exploração e visualização.
