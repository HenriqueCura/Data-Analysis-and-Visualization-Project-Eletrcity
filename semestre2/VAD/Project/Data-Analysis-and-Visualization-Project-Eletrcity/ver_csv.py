# -*- coding: utf-8 -*-
"""
Created on Thu Feb 12 14:47:55 2026

@author: Henrique
"""

import pandas as pd

csv = pd.read_csv('energia-injetada-na-rede-de-distribuicao.csv',delimiter=';')
#csv['Total'] = csv['Outras Tecnologias (kWh)'] + csv['Hídrica (kWh)'] + csv['Fotovoltaica (kWh)'] + csv['Eólica (kWh)']+ csv['Cogeração (kWh)']
#print(sum(csv['Total']==csv['Rede Distribuição (kWh)']))
#diff = csv.loc[csv.loc[:,'Total']!=csv.loc[:,'Rede Distribuição (kWh)']]