def gera_proj_agrup(molecula, num_meses):
    #%% Importando bibliotecas
    import pandas as pd
    import numpy as np
    from pycaret.time_series import TSForecastingExperiment
    from dateutil.relativedelta import relativedelta
    from tqdm import tqdm
    from fn.db_connection import db_connection

    # Get the database connection
    engine = db_connection()
    
    #%% Query molecule list
    def consulta_db():
        query = """SELECT
                    	FF1.molecula
                    FROM
                    	int_mercado_nc.lista_nobrega AS FF1
                    LEFT JOIN (
                    	SELECT
                    		DISTINCT molecula
                    	FROM
                    		int_mercado_nc.lista_nobrega_previsoes_moleculas) AS FF2 ON
                    	FF1.molecula = FF2.molecula
                    WHERE
                    	FF2.molecula IS NULL
                    	AND (FF1.prioridade = 1
                    		OR FF1.prioridade = 2)
                    ORDER BY
                    	FF1.molecula"""
        
        results = pd.read_sql(query, engine)
        opcoes = list(results['molecula'])
        return opcoes
    
    #%% Query PMB data
    erros = []
    df_previsoes = pd.DataFrame()
    
    opcao_selecionada_molecula = molecula.lstrip()
    #Loop through selected molecule options
    try:
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
        
        df = pd.read_sql_query(query, engine)
            
        #%% Full Time Series
        # Format DF
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
        
        #%% Cross-validation (conducted in 2023)
        # Build prediction DataFrame
        n = num_meses       
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
        
        # Generate forecast
        def gera_prev(df):
            df_predict = monta_df_prev(df)[0]
            df_y_predito = monta_df_prev(df)[1]
        
            # Define list of columns to forecast
            lista_cols = list(df.columns)
            del lista_cols[0]
            
            # Sort DataFrame by date
            df_orig = df.sort_values(by='dt_periodo')
            df_orig['dt_periodo'] = pd.to_datetime(df_orig['dt_periodo'], format='%Y-%m-%d', dayfirst=True)

            # Set date column as index
            df_orig.set_index('dt_periodo', drop=True, inplace=True)
            
            # Loop through columns to forecast          
            for i in lista_cols:
                df_aux = df_orig[[i]]
                
            # Initialize Time Series Forecasting Experiment
                ## Try different configurations if an exception occurs
                try:
                    exp = TSForecastingExperiment()
                    exp.setup(df_aux, fh=12, session_id=42, html=False)
                except:
                    try:
                        exp = TSForecastingExperiment()
                        exp.setup(df_aux, fold=2, fh=12, session_id=42, html=False)
                    except:
                        exp = TSForecastingExperiment()
                        exp.setup(df_aux, fold=1, fh=12, session_id=42, html=False)
                
                # Compare models and choose the best one
                melhor_modelo = exp.compare_models(sort = 'MAPE', turbo=False, verbose=False)
                modelo = exp.create_model(melhor_modelo)
                                    
                # Create and finalize model (Train, Text and Predict)
                try:
                    y_treino = exp.get_config("y_train")
                    y_teste = exp.get_config("y_test")

                    # Predict results
                    y_predito = exp.predict_model(modelo)
                    
                    # Handle index
                    y_teste.index = y_teste.index.strftime('%Y-%m-%d')
                    y_teste.index = pd.to_datetime(y_teste.index)
                    new_index = y_teste.index - pd.offsets.MonthBegin(1)
                    y_teste.index = new_index
                    y_treino.index = y_treino.index.to_timestamp()
                    y_predito.index = y_predito.index.to_timestamp()
                    
                    # Concatenate results
                    y_predito.reset_index(inplace=True)
                    y_predito.rename(columns={'index':'dt_periodo','y_pred':i}, inplace=True)
                    df_y_predito = pd.merge(df_y_predito, y_predito, on='dt_periodo', how='left')
                
                    # Tunando modelo
                    modelo_tunado = exp.tune_model(modelo)
                    modelo_final = exp.finalize_model(modelo_tunado)
                    previsoes = exp.predict_model(modelo_final,fh=n)
                    
                    # Realizando merge do resultado
                    previsoes.index = previsoes.index.to_timestamp()
                    previsoes.reset_index(inplace=True)
                    previsoes.rename(columns={'index':'dt_periodo','y_pred':i}, inplace=True)
                    df_predict = pd.merge(df_predict, previsoes, on='dt_periodo', how='left')
                    
                except:
                    pass

            # Adjust negative values to zero
            df_predict.iloc[:,1:] = df_predict.iloc[:,1:].applymap(lambda x: 0 if x < 0 else x)

            # Return prediction and y_predicted DataFrames
            return [df_predict, df_y_predito]
        
        resultado_full = gera_prev(df_formatado)
        df_predict_full, df_y_predito_full = resultado_full[0], resultado_full[1]
        
        resultado_antes = gera_prev(df_antes_pan)
        df_predict_antes, df_y_predito_antes = resultado_antes[0], resultado_antes[1]
        
        resultado_apos = gera_prev(df_apos_pan)
        df_predict_apos, df_y_predito_apos = resultado_apos[0], resultado_apos[1]
        
        #%% Best Period Frame
        def validacao(df):
            df['dt_periodo'] = pd.to_datetime(df['dt_periodo'], format='%Y-%m-%d', dayfirst=True)
            data_atual = max(df['dt_periodo'])
            data_validacao = data_atual - relativedelta(months=6)
            df_validação = df[df['dt_periodo'] > data_validacao]
            df_validação.index = range(6)
            return df_validação
        
        df_validacao = validacao(df_formatado)
        
        #%% Calculate MAPE
        def calcula_mape(y_predito):
            mape = np.mean(np.abs((df_validacao.iloc[:, 1:] - y_predito.iloc[:, 1:]) / df_validacao.iloc[:, 1:])) * 100
            return mape
        
        mape_full = calcula_mape(df_y_predito_full)
        mape_antes = calcula_mape(df_y_predito_antes)
        mape_apos = calcula_mape(df_y_predito_apos)
        
        df_mape_full = pd.DataFrame(columns=['Value'])
        df_mape_antes = pd.DataFrame(columns=['Value'])
        df_mape_apos = pd.DataFrame(columns=['Value'])
        
        # Filter DataFrames based on MAPE comparison
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
        
        # Concatenate DataFrames and insert molecule column
        df_predict_final = pd.concat([df_predict_full.iloc[:, 0], df_predict_full_filtrado, df_predict_antes_filtrado, df_predict_apos_filtrado], axis=1)
        df_predict_final.insert(0, 'molecula', opcao_selecionada_molecula)
        

        # Drop rows with missing values
        df_predict_final.dropna(inplace=True)

        # Concatenate results
        df_previsoes = pd.concat([df_previsoes, df_predict_final], ignore_index=True)

    # Handle exceptions and append to errors list
    except Exception as e:
        erros.append({'molecula': opcao_selecionada_molecula, 'Erro': str(e)})

    # Uncomment if you want to save the predictions to the database
    #df_erros = pd.DataFrame(erros)

    # Return DataFrame with predictions
    return df_previsoes


    

    

    

    

