import sqlite3

def criar_banco():
    conn=sqlite3.connect("log.db")
    cursor=conn.cursor()
    cursor.execute("""
            CREATE TABLE log (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                pdfgerado varchar(255),
                data_da_geracao DATETIME,
                grupo varchar(255)                         
            )""")
    print('Tabela criada com sucesso !!')
    cursor.close()

def excluir_banco():
    conn=sqlite3.connect("log.db")
    cursor=conn.cursor()
    cursor.execute('DROP TABLE log')
    print('Banco excluido com sucesso !!')
    cursor.close()
    


def update_banco():
    conn=sqlite3.connect("log.db")
    cursor=conn.cursor()
    cursor.execute(" ALTER TABLE log ADD COLUMN grupo")
    print('Update  feito com sucesso !!')
    cursor.close()
    conn.close()
    



def drop_banco():
    conn=sqlite3.connect("log.db")
    cursor=conn.cursor()
    cursor.execute(" ALTER TABLE log DROP COLUMN grupo")
    print('Coluna Deletada  feito com sucesso !!')
    cursor.close()
    conn.close()
    


def table_log():
    # Conectando ao banco de dados (ou criando se não existir)
    conn = sqlite3.connect("log.db")
    cursor = conn.cursor()

    # Corrigindo a criação da tabela
    cursor.execute("""
        CREATE TABLE  grupo (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            nome VARCHAR(255),
            id_grupo INT
        )
    """)

    print('Tabela criada com sucesso !!')
    
    # Fechando a conexão
    conn.close()

import sqlite3

def insert_grupo():
    # Conectando ao banco de dados
    conn = sqlite3.connect("log.db")
    cursor = conn.cursor()

    # Listas com os dados
    nome = ['UN_FOLHOSAS_CONVENCIONAIS', 'UN_ORGÂNICO', 'UN_LEGUMES_CONVENCIONAIS', 
            'XP - (INATIVO)', 'UN_AGROINDUSTRIA', 'CAIXAS', 'UN_FRUTAS_CONVENCIONAIS']
    handle = [1, 2, 3, 4, 5, 6, 7]

    # Query de inserção
    query = "INSERT INTO grupo (nome, id_grupo) VALUES (?,?)"
    
    # Loop para inserir os valores
    for n, h in zip(nome, handle):
        cursor.execute(query, (n, h))

    # Confirmando as mudanças
    conn.commit()
    print("Inserções feitas com sucesso!")

    # Fechando a conexão
    conn.close()

# Chamar a função para inserir os dados
insert_grupo()
