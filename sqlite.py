import sqlite3
import json

def altertable():
    try:
        conn=sqlite3.connect('portalcompras.db')
        c=conn.cursor()
        c.execute('ALTER TABLE mensagens_whatsapp ADD COLUMN ACOES TEXT;')
        conn.commit
        conn.close()
        return print('Coluna adicionada com sucesso !!') 
        
    except Exception as e:
        return print(e)


def criar_msg_padrao():
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute(""" 
            CREATE TABLE IF NOT EXISTS msg_padrao (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                conteudo TEXT NOT NULL,  
                grupo TEXT, 
                data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
                descricao TEXT
            );
        """)
        conn.commit()
        conn.close()
        return json.dumps({"mensagem": "banco criado"})
    except Exception as error:
        return json.dumps({"erro": str(error)})
    
def droptable():
    try:
        # Conecte ao banco de dados
        conexao = sqlite3.connect('portalcompras.db')
        cursor = conexao.cursor()

        # Execute o comando para deletar a tabela
        cursor.execute("DROP TABLE IF EXISTS unificado")

        # Confirme as mudanças e feche a conexão
        conexao.commit()
        conexao.close()
        return json.dumps({"mensagem": "banco deletado  com sucesso"})
    except Exception as e:
        return json.dumps({'erro':e})

def criandolograbbitmq():
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute(""" 
            CREATE TABLE IF NOT EXISTS lograbbitmq (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                Numero_grupo_enviado TEXT,  
                mensagem TEXT, 
                data_de_envio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                
            );
        """)
        conn.commit()
        conn.close()
        return print('Tabela Criada com suceeso!!')
    except Exception as error:
        print('Erro ao criar tabela:',error)
        
def criandotableunificado():
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute(""" 
            CREATE TABLE IF NOT EXISTS unificado (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                handle INT  NOT NULL,
                produtor_relatorio TEXT,
                id_grupo TEXT,
                nome_grupo TEXT,
                data_de_envio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                
            );
        """)
        conn.commit()
        conn.close()
        return print('Tabela Criada com suceeso!!')
    except Exception as error:
        print('Erro ao criar tabela:',error)
        


def deleteunificar():
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute("""DELETE FROM unificado""")
        conn.commit()
        conn.close()
        return print('Tabela deletada com suceeso!!')
    except Exception as error:
        print('Erro ao deletar tabela:',error)
        
delete=droptable()  
create=criandotableunificado()

print(delete)
print(create)