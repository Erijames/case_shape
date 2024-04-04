# Import
import pandas as pd
import re

# criar uma classe 
class DataPipeline:
    def __init__(self):
        pass

    # Criar metodoo
    def load_data(self):
        df_json = pd.read_json('equipment.json')
        df_csv = pd.read_csv('equipment_sensors.csv')
        df_txt = pd.read_csv('/content/equpment_failure_sensors.txt', header=None, sep='\t')
        # dar nomes as colunas do arquivo TXT
        df_txt.columns = ['timestamp','error_type','sensor_id','temperature','vibration','']
        return df_json, df_csv, df_txt

    # extrair o número do sensor_id 
    def extract_sensor_id(self, sensor_str):
        # Aqui, vamos procurar por um número dentro de colchetes
        match = re.search(r'\[(\d+)\]', sensor_str)
        if match:
            return int(match.group(1))
        else:
            return None

    # preparar nossos dados do arquivo TXT, aplicando a extração do sensor_id
    def preprocess_data(self, df_txt):
        # função extract_sensor_id para cada valor na coluna sensor_id
        df_txt['sensor_id'] = df_txt['sensor_id'].apply(self.extract_sensor_id)
        return df_txt

    # analisar as falhas, calcular estatisticas e classificar os sensores
    def analyze_failures(self, df_txt, df_json, df_csv):
        # Vamos contar quantas vezes cada sensor falhou
        failures_per_sensor = df_txt.groupby('sensor_id')['error_type'].count()
        # contar quantos sensores falharam pelo menos uma vez
        total_failures = (failures_per_sensor > 0).sum()
        #equipamento teve mais falhas
        equipment_most_failures = df_json['name'].value_counts().idxmax()

        # mesclar  DataFrames para ter todas as informacoes 
        merged_df = df_json.merge(df_csv, how='inner', left_on='equipment_id', right_on='equipment_id')
        merged_df = merged_df.merge(df_txt, how='inner', left_on='equipment_id', right_on='sensor_id')

        # numero de falhas por grupo de equipamentos
        failures_per_group = merged_df.groupby('group_name').size()
        # media de falhas por grupo 
        avg_failures_per_group = failures_per_group.mean()

        # classificar os sensores com mais falhas 
        rank_sensors_by_failures = df_json.merge(df_csv, how='inner', left_on='equipment_id', right_on='equipment_id')
        rank_sensors_by_failures = rank_sensors_by_failures.merge(df_txt, how='inner', left_on='equipment_id', right_on='sensor_id')
        rank_sensors_by_failures = rank_sensors_by_failures.groupby(['name', 'group_name']).size().reset_index(name='sensor_failures')
        rank_sensors_by_failures = rank_sensors_by_failures.sort_values(by=['group_name', 'name', 'sensor_failures'], ascending=[True, True, False])
        rank_sensors_by_failures.reset_index(drop=True, inplace=True)

        # retornar todas os calculos
        return total_failures, equipment_most_failures, avg_failures_per_group, rank_sensors_by_failures

if __name__ == "__main__":
    # instancia da classe 
    pipeline = DataPipeline()
    # carregar nossos dados dos arquivos
    df_json, df_csv, df_txt = pipeline.load_data()
    df_txt = pipeline.preprocess_data(df_txt)
    total_failures, equipment_most_failures, avg_failures_per_group, rank_sensors_by_failures = pipeline.analyze_failures(df_txt, df_json, df_csv)

    # exibir nossas estatisticas para ver o que descobrimos
    print("Total de falhas de equipamento:", total_failures)
    print("Equipamento com mais falhas:", equipment_most_failures)
    print("Média de falhas por grupo de equipamentos:")
    print(avg_failures_per_group)
    print("Rank dos sensores com mais falhas por nome do equipamento em um grupo de equipamentos:")
    print(rank_sensors_by_failures)
