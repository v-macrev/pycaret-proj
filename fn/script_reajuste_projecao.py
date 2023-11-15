# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 14:27:58 2023

@author: a0040447
"""
import pandas as pd

def gera_proj_ajustada(proj_agrup, proj_seg):
    
    def calcula_percent(df_seg):
        df_percent = pd.DataFrame()
        
        df_seg['dt_periodo'] =  pd.to_datetime(df_seg['dt_periodo']).dt.date.astype('str')
        df_seg['CHAVE_MOL_DATA'] = df_seg[['molecula','dt_periodo']].apply('_'.join, axis=1)
        lista_chaves = df_seg.CHAVE_MOL_DATA.unique().tolist()
        
        for chave in lista_chaves:
            df_aux = df_seg.query(f'CHAVE_MOL_DATA == "{chave}"')
            soma_total = df_aux['qt_und'].sum()
            df_aux['porcentagem'] = df_aux['qt_und'] / soma_total
            df_percent = pd.concat([df_percent, df_aux])
        return df_percent
    
    df_seg_percent = calcula_percent(proj_seg)

    #%%
    def retorna_proj_recalculada(df_ag, df_seg_percent):
        df_ag['dt_periodo'] =  pd.to_datetime(df_ag['dt_periodo']).dt.date.astype('str')
        df_ag['CHAVE_MOL_DATA'] = df_ag[['molecula','dt_periodo']].apply('_'.join, axis=1)
        
        df_merge = pd.merge(df_ag, df_seg_percent[['CHAVE_MOL_DATA', 'vertical','porcentagem']], on='CHAVE_MOL_DATA' ,how='left')
        df_merge['qt_unidade_recalculado'] = (df_merge['qt_unidade'] * df_merge['porcentagem']).astype('int64')
        df_merge = df_merge[['dt_periodo','molecula', 'vertical','qt_unidade_recalculado']]
        df_merge.rename(columns={'qt_unidade_recalculado':'qt_unidade'}, inplace=True)
        df_merge['dt_periodo'] = pd.to_datetime(df_merge['dt_periodo'], format='%Y-%m-%d', dayfirst=True)
        return df_merge
    
    df_final = retorna_proj_recalculada(proj_agrup, df_seg_percent)
    
    return df_final