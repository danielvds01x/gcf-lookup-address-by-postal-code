from google.cloud import bigquery
from geopy.geocoders import Nominatim
from datetime import datetime
from os import environ
import requests
import json
import time
import re

QTD_CEPS_CHAMADA = 100
SQL_LIMIT_ROWS = 2000
CLOUD_FUNCTION_ENDPOINT = environ['CLOUD_FUNCTION_ENDPOINT']
TOKEN_CEPABERTO = environ['TOKEN_CEPABERTO'] # TODO : Remover Token de Conta Pessoal e colocar nas Variáveis da CF

# Destino Ceps Corretos
CEP_CORRETO_PROJETO = environ['PROJETO']
CEP_CORRETO_DATASET = 'idd_bi_container_assistant_support'
CEP_CORRETO_TABELA = 'idd_bi_street_address'

# Destino Ceps Inválidos 
CEP_INVALIDO_PROJETO = environ['PROJETO']
CEP_INVALIDO_DATASET = 'idd_bi_container_assistant_support'
CEP_INVALIDO_TABELA = 'idd_bi_street_address_invalid_zipcodes'

def main(request):
    
    content_type = request.headers['content-type']

    if content_type == 'application/json':
        request_json = request.get_json(silent=True)

        # TODO: Requisição com uma Tabela do BQ
        if request_json and 'tabela_bq' in request_json:
            campo_cep = request_json['tabela_bq']['campo_cep']
            projeto = request_json['tabela_bq']['projeto']
            dataset = request_json['tabela_bq']['dataset']
            nome_tabela = request_json['tabela_bq']['nome_tabela']
            query_string = f"""
                WITH lista_ceps AS (
                SELECT DISTINCT
                    LPAD(REGEXP_REPLACE({campo_cep}, '[^0-9]', ''),8,'0') AS {campo_cep}
                FROM 
                    `{projeto}.{dataset}.{nome_tabela}`
                WHERE 
                    LEFT(LPAD(REGEXP_REPLACE({campo_cep}, '[^0-9]', ''),8,'0'),5) <> '00000'
                ORDER BY 1 
                LIMIT {SQL_LIMIT_ROWS}
                )

                SELECT 
                    lista_ceps.{campo_cep}
                FROM 
                    lista_ceps  
                LEFT OUTER JOIN `{CEP_CORRETO_PROJETO}.{CEP_CORRETO_DATASET}.{CEP_CORRETO_TABELA}` enderecos
                    ON lista_ceps.{campo_cep} = enderecos.zipcode 
                LEFT OUTER JOIN `{CEP_INVALIDO_PROJETO}.{CEP_INVALIDO_DATASET}.{CEP_INVALIDO_TABELA}` ceps_invalidos
                    ON lista_ceps.{campo_cep} = ceps_invalidos.zipcode 
                WHERE 
                    enderecos.zipcode IS NULL
                    AND
                    ceps_invalidos.zipcode IS NULL
            """

            # TODO Remover
            print(f'\nQuery: {query_string}')

            # dataframe com os ceps a serem trabalhados
            bqclient = bigquery.Client()
            df = bqclient.query(query_string).result().to_dataframe()

            # quantidade de ceps
            print(f'\nQuantidade de Ceps: {len(df.index)}.\n')

            ceps_invalidos = [ None, r'\N', r'00000000' ]
            ceps = {}
            ceps["tabela_origem"] = f"{projeto}.{dataset}.{nome_tabela}"
            ceps["ceps"] = []

            for index, row in df.iterrows():
                if (index + 1) % QTD_CEPS_CHAMADA != 0:
                    if row[campo_cep] in ceps_invalidos:
                        continue
                    else:
                        cep = re.sub("[^0-9]", "", row[campo_cep]).zfill(8)
                        ceps["ceps"].append(cep)
                else:
                    cep = re.sub("[^0-9]", "", row[campo_cep]).zfill(8)
                    ceps["ceps"].append(cep)
                    print(ceps) # TODO: Remover
                    ceps_json = json.dumps(ceps) # TODO: Remover
                    print(ceps_json) # TODO: Remover
                    response = requests.post(CLOUD_FUNCTION_ENDPOINT, data=ceps_json, headers={'Content-type': 'application/json'})
                    print(response.status_code) # TODO: Remover
                    ceps["ceps"] = []

            # TODO Teste
            return 'Teste: Response Message!'

        # TODO: Requisição com lista de Ceps
        elif request_json and 'ceps' in request_json:
            tempo_inicio = time.time()
            print(f'Inicio: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

            # ceps = request_json['ceps']

            total_ceps = len(request_json['ceps'])
            print(f'\nTotal de Ceps: {total_ceps} ceps.')

            for cep in request_json['ceps']:
                try:
                    print(f"Buscando Cep : {cep}")
                    buscar_endereco(str(cep))
                except ValueError as err:
                    print(err.args[0])
                    inserir_tabela_ceps_error(cep, request_json['tabela_origem'])
                    continue

            tempo_fim = time.time()
            
            message = f'Fim: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            message = message + f'\nTotal de Ceps: {total_ceps} ceps.'
            message = message + f'\nTempo estimado total: {tempo_fim - tempo_inicio} segundos (~{(tempo_fim - tempo_inicio) / 60} minutos)'
            message = message + f'\nTempo médio por cep: {(tempo_fim - tempo_inicio) / total_ceps} segundos.'

            return message

        else:
            raise ValueError("JSON is invalid, or missing some field...")


def inserir_tabela_ceps(cep:str,
        logradouro:str=None,
        complemento:str=None,
        bairro:str=None,
        cidade:str=None,
        estado:str=None,
        latitude:str=None,
        longitude:str=None,
        openstreetmap_flag:int=None):
    """
    Insere um endereço na tabela de ceps no bigquery, gerando um job no BQ a partir de uma query.

    Parameters
    ----------
    cep : str
        Cep a ser inserido na tabela
    logradouro : str , optional
        Nome da rua (campo street)
    complemento : str , optional
        Complemento do endereço, faixa de cep (campo street_range)
    bairro : str , optional
        Nome do bairro (campo district)
    cidade : str , optional
        Nome da cidade (campo city)
    estado : str , optional
        Sigla do estado (campo state)
    latitude : str , optional
        Latitude (campo latitude)
    longitude : str , optional
        Longitude (campo longitude)
    openstreetmap_flag : int , optional
        Flag True/False que indica se o Lat-Lon foi encontrado no OpenStreetMap (info tende a ser mais acurada)

    Returns
    -------
    None
    """

    client = bigquery.Client()

    query_string = f"""
        MERGE `{CEP_CORRETO_PROJETO}.{CEP_CORRETO_DATASET}.{CEP_CORRETO_TABELA}` TARGET
        USING (SELECT NULLIF('{cep}','None') as zipcode,
                      NULLIF('{logradouro}','None') as street,
                      NULLIF('{complemento}','None') as street_range,
                      NULLIF('{bairro}','None') as district,
                      NULLIF('{cidade}','None') as city,
                      NULLIF('{estado}','None') as state,
                      SAFE_CAST('{latitude}' AS NUMERIC) as latitude,
                      SAFE_CAST('{longitude}' AS NUMERIC) as longitude,
                      CASE WHEN {openstreetmap_flag} = 1 THEN True ELSE False END AS openstreetmap_flag,
                      CURRENT_DATE() as ingestion_date,
                      SAFE_CAST(FORMAT_TIMESTAMP('%H:%M:%E*S', CURRENT_TIMESTAMP()) AS TIME) as ingestion_hour
                      ) SOURCE
        ON TARGET.zipcode = SOURCE.zipcode
        -- WHEN MATCHED THEN
        --     UPDATE SET TARGET.cep = SOURCE.cep,
        --            TARGET.logradouro = SOURCE.logradouro,
        --            TARGET.insert_datetime = SOURCE.insert_datetime
        WHEN NOT MATCHED THEN
            INSERT (zipcode,
                    street,
                    street_range,
                    district,
                    city,
                    state,
                    latitude,
                    longitude,
                    openstreetmap_flag,
                    ingestion_date,
                    ingestion_hour)
            VALUES (SOURCE.zipcode,
                    SOURCE.street,
                    SOURCE.street_range,
                    SOURCE.district,
                    SOURCE.city,
                    SOURCE.state,
                    SOURCE.latitude,
                    SOURCE.longitude,
                    SOURCE.openstreetmap_flag,
                    SOURCE.ingestion_date,
                    SOURCE.ingestion_hour)
    """
    # print(query_string)
    query_job = client.query(
    query_string,
    )
    print("Started job (Inserting Bigquery Table): {}".format(query_job.job_id))


def inserir_tabela_ceps_error(cep:str,
        table_origin:str=None):
    """
    Insere o cep na tabela de ceps inválidos no bigquery, gerando um job no BQ a partir de uma query.

    Parameters
    ----------
    cep : str
        Cep a ser inserido na tabela
    table_origin : str , optional
        Tabela no Bigquery onde foi coletado o cep inválido

    Returns
    -------
    None
    """
    
    client = bigquery.Client()

    query_string = f"""
        MERGE `{CEP_INVALIDO_PROJETO}.{CEP_INVALIDO_DATASET}.{CEP_INVALIDO_TABELA}` TARGET
        USING (SELECT NULLIF('{cep}','None') as zipcode,
                      NULLIF('{table_origin}','None') as table_origin,
                      CURRENT_DATE() as ingestion_date,
                      SAFE_CAST(FORMAT_TIMESTAMP('%H:%M:%E*S', CURRENT_TIMESTAMP()) AS TIME) as ingestion_hour
                      ) SOURCE
        ON TARGET.zipcode = SOURCE.zipcode
        -- WHEN MATCHED THEN
        --     UPDATE SET TARGET.zipcode = SOURCE.zipcode
        WHEN NOT MATCHED THEN
            INSERT (zipcode,
                    table_origin,
                    ingestion_date,
                    ingestion_hour)
            VALUES (SOURCE.zipcode,
                    SOURCE.table_origin,
                    SOURCE.ingestion_date,
                    SOURCE.ingestion_hour)
    """
    # print(query_string)
    query_job = client.query(
    query_string,
    )
    print("Started job (Inserting Bigquery Table Errors): {}".format(query_job.job_id))


def buscar_endereco(cep:str):
    """
    - Busca pelo endereço na API cepaberto.com.br, a partir de um cep fornecido.
    - Caso encontre o cep, obtém as informações do endereço e tenta complementar a informação de
        latitude e longitude buscando este endereço encontrado (através da lib geopy.geocoders usando
        a classe Nominatin) na API do OpenStreetMap.
    - Com o endereço encontrado, chama a função inserir_tabela_ceps().
    - Obs.: A busca complementar no OpenStreetMap foi implementada na tentativa de melhorar a qualidade
        da informação de Lat/Lon.

    Parameters
    ----------
    cep : str
        Cep a ser buscado

    Returns
    -------
    None
    """

    # API cepaberto.com.br
    response = None
    url = f"https://www.cepaberto.com/api/v3/cep?cep={cep}"
    headers = {'Authorization': f'Token token={TOKEN_CEPABERTO}'}
    response = requests.get(url, headers=headers)
    
    # Sleep
    time.sleep(1)

    if response.json().get('cep') is not None:
        geolocator = Nominatim(user_agent="my-app")
        # Formatar endereço para busca no OpenStreetMap
        endereco = ' '.join([response.json().get('cep'),
            response.json().get('logradouro') if response.json().get('logradouro') is not None else '',
            response.json().get('cidade').get('nome') if response.json().get('cidade').get('nome') is not None else ''])

        # TODO Remover
        print(f'Endereço encontrado: {endereco}')
        print(f'OpenStreetMap (buscando latitude e longitude)')

        # Buscar Latitude e Longitude com o enedereço encontrado (OpenStreetMap)
        location = geolocator.geocode(endereco)

        # Log
        print('OpenStreetMap, não encontrado') if location is None else print(f'Cep: {cep} - lat: {location.latitude}, lon: {location.longitude}')

        inserir_tabela_ceps(response.json().get('cep'),
            response.json().get('logradouro'),
            response.json().get('complemento'),
            response.json().get('bairro'),
            response.json().get('cidade').get('nome') if response.json().get('cidade').get('nome') is not None else None,
            response.json().get('estado').get('sigla'),
            response.json().get('latitude') if location == None else location.latitude,
            response.json().get('longitude') if location == None else location.longitude,
            0 if location == None else 1)

    else:
        raise ValueError('Erro: Cep não encontrado')


if __name__ == '__main__':
    main()
