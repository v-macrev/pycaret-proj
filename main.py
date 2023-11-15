# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 13:42:29 2023

@author: a0040447, v0042447
"""

import os
import getpass
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

user= getpass.getuser()

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), os.path.basename(os.path.dirname(os.path.abspath(__file__))))

# Change the current directory to the 'fn' folder within the project
fn_folder = os.path.join(BASE_DIR, 'fn')
os.chdir(fn_folder)

# Construct the absolute path for the 'models' folder relative to the project root
model_folder = os.path.join(BASE_DIR, 'models')

from script_projecao_agrupado import gera_proj_agrup
from script_projecao_segmento import gera_proj_seg
from script_reajuste_projecao import gera_proj_ajustada



# get current date and time
start = datetime.now()

# Define the number of retries
max_retries = 3

# Define the delay between retries (in seconds)
retry_delay = 5

lista_consulta = 'ETODOLACO'
n = 12

# convert to string in the desired format
start_str = start.strftime("%Y-%m-%d %H:%M:%S")
print('Período em meses: ' + str(n))
print('O Processo começou em : ' + start_str)

proj_agrup = gera_proj_agrup(lista_consulta, n, model_folder)
print('\nProjeção Agrupada Finalizada!\n')

proj_seg = gera_proj_seg(lista_consulta, n, model_folder)
print('\nProjeção Segmentada Finalizada!\n')

""" 
for retry_count in range(max_retries):
    try:
        proj_seg = gera_proj_seg(lista_consulta, n)
        print('\nProjeção Segmentada Finalizada!\n')
        break  # Successful, exit the loop
    except Exception as e:
        if retry_count < max_retries - 1:
            print(f"Projeção Segmentada falhou (Tentativa {retry_count + 1}). Retentando após {retry_delay} segundos...")
            time.sleep(retry_delay)
        else:
            print("Projeção Segmentada falhou após várias tentativas.")

"""

projecao_final = gera_proj_ajustada(proj_agrup, proj_seg)
print('\nProjeção Final Finalizada!\n')

# get end time
end = datetime.now()
end_str = end.strftime("%Y-%m-%d %H:%M:%S")
print('O Processo terminou em : ' + end_str)

date_str = end.strftime("%Y-%m-%d_%H-%M-%S")

# calculate and print the total time taken
total_time = end - start
print('O Processo levou: ', total_time)

projecao_final.to_excel(rf'C:\Users\{user}\Downloads\projecao_{lista_consulta}.xlsx', index=False)