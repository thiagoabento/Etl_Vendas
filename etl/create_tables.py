import sqlite3
import configparser

class CriaTabelaResumo:
    def tabela_resumo_estado(self):
        config = configparser.ConfigParser()
        config.read('../config.ini')
        db_patch = config['sqlite']['database']

        conn = sqlite3.connect(db_patch)
        cursor = conn.cursor()

        conn.execute('CREATE TABLE resuvendascat(TOTVENDAS REAL, NMCATEGORIA TEXT)')

        print('Tabela criada com sucesso')
        conn.commit()
        conn.close()