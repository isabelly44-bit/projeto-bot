import requests
import pandas as pd
import os

# Função para capturar os metadados do JSON
def metadata_capture(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# Função para processar os metadados e salvar tudo em um arquivo de saída com a extensão .parquet
def process_and_save_metadata(metadata_json, save_path):
    registros = []

    # Extrair variáveis
    variaveis = metadata_json.get('Variaveis', [])
    if variaveis:
        df_variaveis = pd.json_normalize(
            variaveis, 
            record_path=['UnidadeDeMedida'], 
            meta=['Id', 'Nome', 'Desidentificacao']
        )
        df_variaveis['tipo_dado'] = 'Variavel'
        registros.append(df_variaveis)
        print("Variáveis processadas.")
    else:
        print("Nenhuma variável encontrada.")

    # Extrair territórios
    territorios = metadata_json.get('Territorios')
    if territorios:
        df_territorios = pd.json_normalize(territorios)
        df_territorios['tipo_dado'] = 'Territorio'
        registros.append(df_territorios)
        print("Territórios processados.")
    else:
        print("Nenhum território encontrado.")

    # Extrair data de atualização
    update_date = metadata_json.get('DataAtualizacao')
    if update_date:
        df_update = pd.DataFrame([{'DataAtualizacao': update_date, 'tipo_dado': 'Atualizacao'}])
        registros.append(df_update)
        print("Data de atualização processada.")
    else:
        print("Data de atualização não encontrada.")

    # Concatenar tudo em um único DataFrame
    df_final = pd.concat(registros, ignore_index=True)

    # Salvar como Parquet
    df_final.to_parquet(save_path, index=False)
    print(f"Arquivo salvo: {save_path}")

# Função principal que chama todas as outras anteriores
def main():
    url = "https://sidra.ibge.gov.br/Ajax/Json/Tabela/1/1737?versao=-1"
    save_dir = r"C:\Users\Samsung\OneDrive\Documentos\ipca\metadados"
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, "metadados_completo.parquet")

    print("Capturando metadados...")
    metadata_json = metadata_capture(url)

    print("Processando e salvando metadados em arquivo único...")
    process_and_save_metadata(metadata_json, save_path)

    print("FINALIZADO! SEUS DADOS ESTÃO SALVOS E PRONTOS PARA SEREM UTILIZADOS! ;)")

if __name__ == "__main__":
    main()

    # OBSERVAÇÃO:
    # O método de raciocínio utilizado aqui foi extrair as informações estruturais do JSON para a tabela. Por esse motivo, considerei puxar as informações como território, data de atualização
    # e demais informações separadamente e concatena-las para gerar o arquivo final em parquet.