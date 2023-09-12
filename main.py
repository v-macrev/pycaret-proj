#%% Starting Scripts
import getpass
import pandas as pd
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from ttkwidgets.autocomplete import AutocompleteCombobox
from fn.db_connection import db_connection

# Get the database connection
engine = db_connection()

def handle_error(error_message):
    messagebox.showerror("Error", error_message)


def consulta_db_mol():
    query = """select distinct dc_molecula_portugues from pmb.dm_mole_pmb order by dc_molecula_portugues"""
    
    results = pd.read_sql(query, engine)
    moleculas = list(results['dc_molecula_portugues'])
    return moleculas

# Função para gerar as projeções
def gerar_projecoes():
    # Obtendo os valores dos widgets
    molecula = molula_combobox.get().upper()
    num_meses = int(num_meses_entry.get())
    # Update progress bar
    progress_bar["value"] = 0  # Reset progress bar
    janela.update_idletasks()

    def progress_callback(progress):
            progress_bar["value"] = progress
            janela.update_idletasks()
        
    #%% Importing and executing functions
    try:
    # Rest of your code for generating projections   
        from fn.script_projecao_segmento import gera_proj_seg
        from fn.script_projecao_agrupado import gera_proj_agrup
        from fn.script_reajuste_projecao import gera_proj_ajustada

        
        proj_seg = gera_proj_seg(molecula, num_meses, progress_callback)
        progress_callback(33)  # For example, update to 50% progress

        proj_agrup = gera_proj_agrup(molecula, num_meses)
        progress_callback(66)  # For example, update to 50% progress

        projecao_final = gera_proj_ajustada(proj_agrup, proj_seg)
        progress_callback(99)  # For example, update to 50% progress

        # Save projections to Excel
        projecao_final.to_excel(rf'C:\Users\{user}\Downloads\projecao-{molecula}_{num_meses}-meses.xlsx', index=False)

        # Update progress bar
        progress_callback(100)  #100%

        # Show success message
        messagebox.showinfo("Success", "Projections generated successfully!")

    except Exception as e:
        handle_error(f"Error generating projections: {e}")

# Função para fechar a janela
def fechar_janela():
    janela.destroy()

# Obtendo o nome do usuário
user = getpass.getuser()

# Creating the interface
janela = tk.Tk()
janela.title("Gerar Projeções")

# Labels and entries
molula_label = ttk.Label(janela, text="Nome da Molécula:")
molula_label.grid(row=0, column=0, padx=5, pady=5)

# Fetching the list of molecules
moleculas = consulta_db_mol()  # Assuming this function works as expected

molecula_var = tk.StringVar()
# Use AutocompleteCombobox instead of ttk.Combobox
molula_combobox = AutocompleteCombobox(janela, textvariable=molecula_var, completevalues=moleculas)
molula_combobox.grid(row=0, column=1, padx=5, pady=5)

num_meses_label = ttk.Label(janela, text="Número de Meses:")
num_meses_label.grid(row=1, column=0, padx=5, pady=5)
num_meses_entry = ttk.Entry(janela)
num_meses_entry.grid(row=1, column=1, padx=5, pady=5)

# Botões
gerar_button = ttk.Button(janela, text="Gerar Projeções", command=gerar_projecoes)
gerar_button.grid(row=2, column=0, padx=5, pady=5)

fechar_button = ttk.Button(janela, text="Fechar", command=fechar_janela)
fechar_button.grid(row=2, column=1, padx=5, pady=5)

# Adding a progress bar
progress_bar = ttk.Progressbar(janela, length=200, mode="determinate")
progress_bar.grid(row=3, column=0, columnspan=2, padx=5, pady=5)

# Adding a label for the progress bar
progress_label = ttk.Label(janela, text="Progress:")
progress_label.grid(row=4, column=0, padx=5, pady=5)

# Iniciando a interface
janela.mainloop()
# %%
