import json
import os
import pandas as pd
import awswrangler as wr
import datetime
import boto3
import time
import pandas as pd
from playwright.sync_api import sync_playwright
from io import StringIO
from dotenv import load_dotenv

# --- Variáveis de Ambiente (Recomendado) ---

load_dotenv()

NOME_BUCKET = os.environ.get('NOME_BUCKET_S3')
URL_ALVO = os.environ.get('URL_SCRAPING')
ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')


def parse_table_bruta(table_html):
    """
    OBJETIVO: PEGAR OS DADOS BRUTOS.
    Esta função foi simplificada ao máximo.
    Ela apenas lê o HTML da tabela e o retorna como um DataFrame,
    sem limpar colunas, sem converter números, sem nenhum tratamento.
    """
    df_bruto = pd.read_html(StringIO(f"<table>{table_html}</table>"))[0]
    return df_bruto

def lambda_handler(event, context):
    """
    Ponto de entrada principal para a execução do AWS Lambda.
    """
    print("Iniciando a execução da função Lambda.")
    
    try:
        # =================================================================
        b3_url = URL_ALVO
        print(f"Iniciando scraping da URL: {URL_ALVO}")
        print("Iniciando o navegador com Playwright...")
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            
            print(f"Acessando a URL: {b3_url}")
            page.goto(b3_url, timeout=90000)

            print("Aguardando a tabela inicial...")
            page.wait_for_selector("table.table-responsive-sm.table-responsive-md", timeout=90000)
            
            # Alterando a consulta para "Setor de Atuação"
            print("Alterando a consulta para Setor de Atuação...")
            page.locator("#segment").select_option("2")
            
            # O valor para "Setor de Atuação" é '2'
            print("Alterando a visualização da pagina para 120...")
            page.locator("#selectPage").select_option("120")
            
            # Aguardando recarregar
            print("Aguardando a tabela ser recarregada (espera de 10 segundos)...")
            time.sleep(10)

            # Captura a data do pregão
            print("Capturando a data do pregão...")
            try:
                data_pregao_texto = page.locator("div#divContainerTabela").inner_text()
                import re
                match = re.search(r"(\d{2}/\d{2}/\d{4})", data_pregao_texto)
                if match:
                    data_pregao = datetime.datetime.strptime(match.group(1), "%d/%m/%Y").strftime("%Y-%m-%d")
                else:
                    data_pregao = datetime.date.today().strftime("%Y-%m-%d")
                print(f"Data do pregão identificada: {data_pregao}")
            except Exception as e:
                print(f"Não foi possível capturar a data do pregão: {e}")
                data_pregao = datetime.date.today().strftime("%Y-%m-%d")

            print("Capturando o HTML da tabela completa...")
            table_selector = "table.table-responsive-sm.table-responsive-md"
            table = page.locator(table_selector)
            table_html = table.inner_html()

            browser.close()
            print("Navegador fechado.")

        # =================================================================
        # Processamento da tabela
        if table_html:
            print("\nProcessando a tabela com o Pandas (somente dados brutos)...")
            try:
                dataframe_final = parse_table_bruta(table_html)
                
                # Adiciona a coluna da data do pregão
                dataframe_final["data_pregao"] = data_pregao
                
                print("--- ✅ Tabela com Dados Brutos do Ibovespa ✅ ---")
                print(dataframe_final)
                
                # Nome do arquivo JSON com a data do pregão
                dados_brutos_scrape_json = f"pregao_{data_pregao}.json"
                
                print(f"\nSalvando os dados em formato JSON como: {dados_brutos_scrape_json}")
                dataframe_final.to_json(
                    dados_brutos_scrape_json,
                    orient='records',
                    indent=4,
                    force_ascii=False
                )
                print(f"✅ Dados brutos salvos com sucesso no arquivo JSON '{dados_brutos_scrape_json}'!")

            except Exception as e:
                print(f"Ocorreu um erro ao processar a tabela com o pandas: {e}")
        else:
            print("❌ Não foi possível capturar o HTML da tabela. O script falhou na etapa de navegação.")
        
        print("Scraping concluído com sucesso.")
        
        # =================================================================
        print("Iniciando o script de ingestão de dados para o S3...")

        try:
            print(f"Lendo o arquivo local: '{dados_brutos_scrape_json}'...")
            df = pd.read_json(dados_brutos_scrape_json)
            print("Arquivo carregado em um DataFrame com sucesso.")
            print(f"Número de registros: {len(df)}")
        except FileNotFoundError:
            print(f"❌ ERRO: O arquivo '{dados_brutos_scrape_json}' não foi encontrado.")
            print("Por favor, execute o script 'scrape_b3_para_json.py' primeiro.")
        except Exception as e:
            print(f"❌ ERRO ao ler o arquivo JSON: {e}")

        # --- ETAPA 3: Colunas de partição ---
        print("\nAdicionando colunas de partição (ano, mes, dia)...")
        data_particao = datetime.date.today()
        df['ano'] = data_particao.year
        df['mes'] = data_particao.month
        df['dia'] = data_particao.day
        print(f"Colunas de partição criadas para a data: {data_particao}")

        # --- ETAPA 4: Definir o destino no S3 ---
        s3_bucket = 'etl-ingestao'
        # Nome no S3 usando a data do pregão
        s3_path_base = f"s3://{s3_bucket}/dados-brutos/pregao_{data_pregao}.parquet"
        print(f"Preparando para escrever os dados em: {s3_path_base}")

        try:
            boto3_session = boto3.Session(
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_ACCESS_KEY,
                region_name='sa-east-1'
            )
            print("Sessão Boto3 criada com as credenciais fornecidas.")
        except Exception as e:
            print(f"Erro ao criar a sessão Boto3: {e}")
            exit()

        try:
            resultado = wr.s3.to_parquet(
                df=df,
                path=s3_path_base,
                dataset=False,
                #mode='append',
                boto3_session=boto3_session
            )
            print("\n✅ Dados ingeridos e salvos no S3 com sucesso!")
            print("Arquivos escritos:")
            for path in resultado['paths']:
                print(f"- {path}")
            
            
        except Exception as e:
            print(f"\n❌ Ocorreu um erro ao escrever os dados no S3: {e}")
        
        print(f"Upload para o bucket {NOME_BUCKET} concluído.")

        return {
            'statusCode': 200,
            'body': json.dumps('Processo de scraping e upload concluído com sucesso!')
        }

    except Exception as e:
        print(f"Ocorreu um erro: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erro na execução: {str(e)}')
        }
        
if __name__ == "__main__":
    print("Iniciando o processo de scraping e ingestão de dados...")
    lambda_handler(event=None, context=None)
    print("Processo concluído com sucesso!")