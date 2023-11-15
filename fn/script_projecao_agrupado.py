# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 11:15:28 2023

@author: a0040447, v0042447
"""

import logging
import joblib
import hashlib
from datetime import datetime

# Importando bibliotecas
import pandas as pd
import numpy as np
import gc
from pycaret.time_series import TSForecastingExperiment
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from tqdm import tqdm
from connect import conn
connections = conn()

logger = logging.getLogger(__name__)
    

# Instead of print('message'), use:
logger.info('message')

def gera_proj_agrup(lista_consulta, n, model_folder):
    try:
        logger.info("Starting the gera_proj_seg function")
    
        now = datetime.now()
        month = now.strftime("%m")
        year = now.strftime("%Y")

        # Consulta lista mercado montado
        def consulta_db():
            logger.info("Starting consulta_db function")
            try:
                with connections.cursor() as cursor:
                    query = """
                        select distinct dc_molecula_portugues
                        from pmb.dm_mole_pmb
                        order by dc_molecula_portugues
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()
                    columns = [col[0] for col in cursor.description]
                    df = pd.DataFrame(results, columns=columns)
                opcoes = df['dc_molecula_portugues'].tolist()
                return opcoes
            finally:
                cursor.close()
        
        # Consulta dados pmb
        erros = []
        df_previsoes = pd.DataFrame()
        
        def generate_shortened_identifier(lista_consulta):
            concatenated_string = '_'.join(lista_consulta)
            sha256_hash = hashlib.sha256(concatenated_string.encode()).hexdigest()
            return sha256_hash

        opcao_selecionada_molecula = lista_consulta.lstrip()
        molecula = generate_shortened_identifier(opcao_selecionada_molecula)

        logger.info("Starting the query")
        query = f"""select
                        FF.dt_periodo,
                        sum(FF.qt_unidade) as qt_unidade
                    from
                        ((
                        select
                            PMB.dt_periodo,
                            PMB.cd_fcc,
                            sum(PMB.qt_unidade) as qt_unidade
                        from
                            pmb.fato_pmb as PMB
                        group by
                            PMB.dt_periodo,
                            PMB.cd_fcc)
                    union 
                                        (
                    select
                        cast(PMB2.dt_periodo as date) as dt_periodo,
                        PMB2.cd_fcc,
                        sum(PMB2.qtde) as qt_unidade
                    from
                        int_mercado_nc.pmb_hist_tmp2 as PMB2
                    where
                        PMB2.dt_periodo < (
                        select
                            min(FF2.dt_periodo)
                        from
                            pmb.fato_pmb as FF2)
                    group by
                        PMB2.dt_periodo,
                        PMB2.cd_fcc)) as FF
                    inner join int_mercado_nc.fcc_molecula_concat as MOL on
                        MOL.cd_fcc = FF.cd_fcc
                    where
                        MOL.dc_molecula_portugues like '{opcao_selecionada_molecula}'
                    group by
                        FF.dt_periodo
                    order by
                        FF.dt_periodo"""
        
        print(query)
        logger.debug("Query: %s", query)
        try:
            logger.debug("Conexão Iniciando: %s", query)
            with connections.cursor() as cursor:
                logger.debug("Query Iniciando: %s", query)
                cursor.execute(query)
                results = cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                df = pd.DataFrame(results, columns=columns)
        finally:
                cursor.close()
        
        # Série Temporal Completa
        #Formatar DF
        def formata_df(df):
            df_formatado=df_formatado=df.copy()
            datas_desejadas = pd.date_range(start='2009-01-01', end=max(df_formatado['dt_periodo']), freq='MS')
            df_datas_completas = pd.DataFrame({'dt_periodo': datas_desejadas})
            df_datas_completas['dt_periodo'] = pd.to_datetime(df_datas_completas['dt_periodo']).dt.date
            df_formatado = df_datas_completas.merge(df_formatado, on='dt_periodo', how='left')
            df_formatado['dt_periodo'] = pd.to_datetime(df_formatado['dt_periodo'], format='%Y-%m-%d', dayfirst=True)
            df_formatado = df_formatado.fillna(0)
            return df_formatado

        df_formatado = formata_df(df)
        df_antes_pan = df_formatado[df_formatado['dt_periodo'] <= '2020-03-01']
        df_apos_pan = df_formatado[df_formatado['dt_periodo'] > '2020-03-01']
        df['dt_periodo'] = pd.to_datetime(df['dt_periodo'])
        
        # Validação cruzada (realizado em 2023)
        # Obter a entrada do usuário (periodos a prever)
                
        def monta_df_prev(df):
            data_atual = max(df['dt_periodo'])
            data_inicial = data_atual + relativedelta(months=1)
            data_final = data_atual + relativedelta(months=n)
            data_6_meses = data_atual - relativedelta(months=5)
            
            df_datas = pd.DataFrame({'dt_periodo': pd.date_range(start=data_inicial, end=data_final, freq='D')})
            df_predict = df_datas[df_datas['dt_periodo'].dt.day == 1]

            df_datas2 = pd.DataFrame({'dt_periodo': pd.date_range(start=data_6_meses, end=data_atual, freq='D')})
            df_y_predito = df_datas2[df_datas2['dt_periodo'].dt.day == 1]
            return [df_predict, df_y_predito]
                
        def gera_prev(df, nome_prev):
            df_predict = monta_df_prev(df)[0]
            df_y_predito = monta_df_prev(df)[1]

            # Definindo lista de colunas em que faremos a previsão
            lista_cols = list(df.columns)
            del lista_cols[0]

            #% Tratando o index
            df_orig = df.sort_values(by='dt_periodo')
            df_orig['dt_periodo'] = pd.to_datetime(df_orig['dt_periodo'], format='%Y-%m-%d', dayfirst=True)
            df_orig.set_index('dt_periodo', drop=True, inplace=True)

            
            exp = TSForecastingExperiment()

            for i in lista_cols:
                df_aux = df_orig[[i]]

                try:
                    modelo_salvo = joblib.load(f"{model_folder}/model_agp_{nome_prev}_{molecula}_{month}_{year}.joblib")
                    logger.info(f"Usando modelo salvo: model_agp_{molecula}_{month}_{year}.joblib")

                    previsoes = exp.predict_model(modelo_salvo,fh=n)
                        
                    # Realizando merge do resultado
                    previsoes.index = previsoes.index.to_timestamp()
                    previsoes.reset_index(inplace=True)
                    previsoes.rename(columns={'index':'dt_periodo','y_pred':i}, inplace=True)
                    df_predict = pd.merge(df_predict, previsoes, on='dt_periodo', how='left')
                    
                except Exception as e:
                    logger.info(f"Modelo não encontrado. Treinando novo modelo. Erro: {e}")

                    # Escolhendo melhor modelo estatístico
                    try:
                        exp.setup(df_aux, fh=12, session_id=42, html=False)
                    except:
                        try:
                            exp.setup(df_aux, fold=2, fh=12, session_id=42, html=False)
                        except:
                            exp.setup(df_aux, fold=1, fh=12, session_id=42, html=False)

                    melhor_modelo = exp.compare_models(sort='MAPE', turbo=False, verbose=False)
                    modelo = exp.create_model(melhor_modelo)

                    # Gerando bases de treino teste e previsao
                    y_treino = exp.get_config("y_train")
                    y_teste = exp.get_config("y_test")
                    y_teste.index = y_teste.index.strftime('%Y-%m-%d')
                    y_teste.index = pd.to_datetime(y_teste.index)
                    new_index = y_teste.index - pd.offsets.MonthBegin(1)
                    y_teste.index = new_index
                    y_treino.index = y_treino.index.to_timestamp()

                    y_predito = exp.predict_model(modelo)

                    # Concatenando resultados do y_predito
                    y_predito.reset_index(inplace=True)
                    y_predito.rename(columns={'index': 'dt_periodo', 'y_pred': i}, inplace=True)
                    df_y_predito = pd.merge(df_y_predito, y_predito, on='dt_periodo', how='left')

                    # Tunando modelo
                    modelo_tunado = exp.tune_model(modelo)
                    modelo_final = exp.finalize_model(modelo_tunado)

                    # Salvando modelo
                    model_filename = f"{model_folder}/model_agp_{nome_prev}_{molecula}_{month}_{year}.joblib"
                    joblib.dump(modelo_final, model_filename)
                    logger.info(f"Modelo model_agp_{molecula}_{month}_{year}.joblib salvo com sucesso")

                    previsoes = exp.predict_model(modelo_final, fh=n)

                    # Realizando merge do resultado
                    previsoes.index = previsoes.index.to_timestamp()
                    previsoes.reset_index(inplace=True)
                    previsoes.rename(columns={'index': 'dt_periodo', 'y_pred': i}, inplace=True)
                    df_predict = pd.merge(df_predict, previsoes, on='dt_periodo', how='left')

            df_predict.iloc[:, 1:] = df_predict.iloc[:, 1:].applymap(lambda x: 0 if x < 0 else x)
            return [df_predict, df_y_predito]

        # Assuming df_formatado, df_antes_pan, and df_apos_pan are defined
        resultado_full = gera_prev(df_formatado, "full")
        df_predict_full, df_y_predito_full = resultado_full[0], resultado_full[1]

        resultado_antes = gera_prev(df_antes_pan, "antes")
        df_predict_antes, df_y_predito_antes = resultado_antes[0], resultado_antes[1]

        resultado_apos = gera_prev(df_apos_pan, "apos")
        df_predict_apos, df_y_predito_apos = resultado_apos[0], resultado_apos[1]
        
        # MELHOR RECORTE DE DATAS (Validação de 6 meses)
        def validacao(df):
            df['dt_periodo'] = pd.to_datetime(df['dt_periodo'], format='%Y-%m-%d', dayfirst=True)
            data_atual = max(df['dt_periodo'])
            data_validacao = data_atual - relativedelta(months=6)
            df_validação = df[df['dt_periodo'] > data_validacao]
            df_validação.index = range(6)
            return df_validação
        
        df_validacao = validacao(df_formatado)
        
        # Calcular o Erro Médio Percentual Absoluto (MAPE)
        def calcula_mape(y_predito):
            mape = np.mean(np.abs((df_validacao.iloc[:, 1:] - y_predito.iloc[:, 1:]) / df_validacao.iloc[:, 1:])) * 100
            return mape
        
        mape_full = calcula_mape(df_y_predito_full)
        mape_antes = calcula_mape(df_y_predito_antes)
        mape_apos = calcula_mape(df_y_predito_apos)
        
        df_mape_full = pd.DataFrame(columns=['Value'])
        df_mape_antes = pd.DataFrame(columns=['Value'])
        df_mape_apos = pd.DataFrame(columns=['Value'])
        
        for index, value in mape_full.items():
            if value < mape_antes[index] and value < mape_apos[index]:
                df_mape_full.loc[index] = value

            elif mape_antes[index] < value and mape_antes[index] < mape_apos[index]:
                df_mape_antes.loc[index] = mape_antes[index]
                
            elif mape_apos[index] < value and mape_apos[index] < mape_antes[index]:
                df_mape_apos.loc[index] = mape_apos[index]
                
            elif mape_apos[index] == value and mape_apos[index] == mape_antes[index]:
                df_mape_apos.loc[index] = mape_apos[index]
            
            else:
                df_mape_full.loc[index] = value
        
        df_predict_full_filtrado = df_predict_full.filter(df_mape_full.index.tolist())
        df_predict_antes_filtrado = df_predict_antes.filter(df_mape_antes.index.tolist())
        df_predict_apos_filtrado = df_predict_apos.filter(df_mape_apos.index.tolist())
        
        df_predict_final = pd.concat([df_predict_full.iloc[:, 0], df_predict_full_filtrado, df_predict_antes_filtrado, df_predict_apos_filtrado], axis=1)
        df_predict_final.insert(0, 'molecula', opcao_selecionada_molecula)
        
        #df_despivoteado = pd.melt(df_predict_final, id_vars=['molecula', 'dt_periodo'], var_name='vertical', value_name='qt_und') 
        df_predict_final.dropna(inplace=True)
        df_previsoes = pd.concat([df_previsoes, df_predict_final], ignore_index=True)
        #df_predict_final.to_sql('lista_nobrega_previsoes', engine, schema ='int_mercado_nc',if_exists='append',
        #                       index=False)
    
        #df_erros = pd.DataFrame(erros)
        logger.info("Finished processing the data")

        return df_previsoes
    
    finally:
        del df_formatado
        del df_antes_pan
        del df_apos_pan
        del df_y_predito_full
        del df_y_predito_antes
        del df_y_predito_apos
        del resultado_full
        del resultado_antes
        del resultado_apos
        del df_validacao
        del mape_full
        del mape_antes
        del mape_apos
        del df_mape_full
        del df_mape_antes
        del df_mape_apos
        del df_predict_full_filtrado
        del df_predict_antes_filtrado
        del df_predict_apos_filtrado
        del df_predict_final
        gc.collect()