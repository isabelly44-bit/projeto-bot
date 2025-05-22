## WEB SCRAPER (BOT):

Este projeto consiste em um robô que consome dados do IPCA no IBGE através de uma requisição HTTP ao endpoint JSON fornecido. Os dados capturados são convertidos para um DataFrame utilizando a biblioteca pandas e, posteriormente, salvos em um arquivo no formato Parquet, garantindo armazenamento eficiente e estruturado.

O CÓDIGO ESTÁ MODULARIZADO DA SEGUINTE MANEIRA:
- Uma função para capturar os dados JSON chamada metadata_capture;
- Uma função de processamento e save das informações que foram capturadas chamada process_and_save_metadata;
- Uma função main que executa as anteriores.

O código está documentado e disponível neste repositório. 
