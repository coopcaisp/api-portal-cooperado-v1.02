import sqlite3
import json
import pandas as pd


def droptable():
    try:
        # Conecte ao banco de dados
        conexao = sqlite3.connect('portalcompras.db')
        cursor = conexao.cursor()

        # Execute o comando para deletar a tabela
        cursor.execute("DROP TABLE IF EXISTS mensagens_whatsapp")

        # Confirme as mudanças e feche a conexão
        conexao.commit()
        conexao.close()
        return json.dumps({"mensagem": "banco deletado  com sucesso"})
    except Exception as e:
        return json.dumps({'erro':e})
    

def criar_db():
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute(""" 
            CREATE TABLE IF NOT EXISTS mensagens_whatsapp (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                conteudo TEXT NOT NULL, 
                id_grupo  INTEGER NOT NULL, 
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
    

    
def InserirMensagemPadrao(content):
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute("INSERT INTO msg_padrao (conteudo, grupo, descricao) VALUES (?, ?, ?)", 
                  (content, "padrao", "mensagem padrao para todos os produtores"))
        conn.commit()
        conn.close()
        return json.dumps({"mensagem": "mensagem padrao inserido com sucesso"})
    except Exception as error:
        return json.dumps({"erro": str(error)})
    
    
def UpdateMensagemPadrao(id,content):
    try:
        conn=sqlite3.connect('portalcompras.db')
        c=conn.cursor()
        c.execute("UPDATE msg_padrao SET conteudo=? where id=?",(content,id))
        conn.commit()
        conn.close()
        return json.dumps({"mensagem": "mensagem padrao atualizado com sucesso"})
    except Exception as error:
        return json.dumps({"erro": str(error)})
    
def SelecionarMensagemPadrao():
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute("SELECT * FROM msg_padrao")
        rows = c.fetchall()
        columns = [column[0] for column in c.description]
        df = pd.DataFrame.from_records(rows, columns=columns)
        
        # Convert to list of dictionaries and ensure native data types
        result = df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")
        
        return result
    except Exception as e:
        return json.dumps({"erro": str(e)})
    
def DeletarMensagemPadrao(id):
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute("DELETE FROM msg_padrao WHERE id=?", (id,))
        conn.commit()
        conn.close()
        return json.dumps({"sucesso": "Deletado com sucesso !!"})
    except Exception as error:
        return json.dumps({"erro": str(error)})
        
    
def cadastrar_msg(conteudo, id_grupo, descricao=None):
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute(
            "INSERT INTO mensagens_whatsapp (conteudo, id_grupo, descricao) VALUES (?, ?, ?)",
            (conteudo, id_grupo, descricao)
        )
        conn.commit()
        conn.close()
        return json.dumps({"mensagem": "Mensagem cadastrada com sucesso!"})
    except Exception as error:
        return json.dumps({"erro": str(error)})
    
def update_msg(id, mensagem):
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute("UPDATE mensagens_whatsapp SET conteudo = ? WHERE id=?", (mensagem, id))
        conn.commit()
        conn.close()
        return json.dumps({"sucesso": f"Update realizado no id:{id}"})
    except Exception as error:
        return json.dumps({"erro": str(error)})
    
def delete_msg(id):
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute("DELETE FROM mensagens_whatsapp WHERE id=?", (id,))
        conn.commit()
        conn.close()
        return json.dumps({"sucesso": "Deletado com sucesso !!"})
    except Exception as error:
        return json.dumps({"erro": str(error)})
    
def SelecionarMensagem():
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute("SELECT mensagens_whatsapp.id as id_whats, conteudo,grupo_whats.nome_grupo,grupo_whats.id_grupo, mensagens_whatsapp.ACOES FROM mensagens_whatsapp INNER JOIN grupo_whats ON mensagens_whatsapp.id_grupo = grupo_whats.id")
        rows = c.fetchall()
        columns = [column[0] for column in c.description]
        conn.close()
        
        # Create DataFrame
        df = pd.DataFrame.from_records(rows, columns=columns)
        
        # Convert to list of dictionaries and ensure native data types
        result = df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")
        
        return result  # FastAPI can handle lists of dictionaries
    
    except Exception as e:
        return {"error": str(e)}

        



def criardb_gp():
    try:
        conn=sqlite3.connect('portalcompras.db')
        c=conn.cursor()
        c.execute(""" 
            CREATE TABLE IF NOT EXISTS grupo_whats (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                id_grupo TEXT , 
                nome_grupo TEXT, 
                data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
                descricao TEXT
            );
        """)
        conn.commit()
        conn.close()
        return json.dumps({"message":"Grupo cadastrado"})
    except Exception as error:
        return json.dumps({"erro":error})
    
    
def  inserir_grupo(id_grupo,nome_grupo ,descricao=None):
    try:
        conn=sqlite3.connect('portalcompras.db')
        c=conn.cursor()
        c.execute(
            "INSERT INTO grupo_whats (id_grupo, nome_grupo, descricao) VALUES (?, ?, ?)",
            (id_grupo, nome_grupo, descricao) )
        conn.commit()
        
        conn.close()
        return json.dumps({"message":"messagem inserida com sucesso !!"})
    except Exception as error:
        return json.dump({"error":error})
    
    
def update_gp(id,id_grupo):
    try:
        conn=sqlite3.connect('portalcompras.db')
        c=conn.cursor()
        c.execute("UPDATE grupo_whats SET id_grupo = ? WHERE id=?", (id,id_grupo))
        conn.close()
        conn.commit()
        return json.dumps({"message":"update realizado !!"})
    except Exception as error:
        return json.dumps({"erro":error})
    
def delete_gp(id):
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute("DELETE FROM grupo_whats WHERE id=?", (id,))
        conn.commit()
        conn.close()
        return json.dumps({"message": 'grupo deletado !!'})
    except Exception as error:
        return json.dumps({"error": str(error)})
    
def SelectGrupo():
    try:
        conn = sqlite3.connect('portalcompras.db')
        c = conn.cursor()
        c.execute('SELECT id,id_grupo, nome_grupo,ACOES, descricao FROM grupo_whats')
        rows = c.fetchall()
        columns = [column[0] for column in c.description]
        conn.close()
        
        # Create DataFrame
        df = pd.DataFrame.from_records(rows, columns=columns)
        
        # Convert to list of dictionaries and ensure native data types
        result = df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")
        
        return result  # FastAPI can handle lists of dictionaries
    
    except Exception as e:
        return {"error": str(e)}


def insertunificado(handle,produtor,id,grupo):
    try:
        conn=sqlite3.connect('portalcompras.db')
        c=conn.cursor()
        query="""INSERT INTO unificado (handle,produtor_relatorio,id_grupo,nome_grupo)VALUES(?,?,?,?) """
        c.execute(query,(handle,produtor,id,grupo))
        conn.commit()
        conn.close
        return json.dumps({"message":"inserido com sucesso"})
    except Exception as e:
        return json.dumps({'Error':e})
    
    
def SelectProdutor(produtor:str):
    try:
        conn=sqlite3.connect('portalcompras.db')
        c=conn.cursor()
        sql="""SELECT id,handle,produtor_relatorio,id_grupo,nome_grupo FROM unificado WHERE produtor_relatorio=?"""
        c.execute(sql,(produtor,))
        rows = c.fetchall()
        columns = [column[0] for column in c.description]
        conn.close()
        
        # Create DataFrame
        df = pd.DataFrame.from_records(rows, columns=columns)
        
        # Convert to list of dictionaries and ensure native data types
        result = df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")
        
        return result  # FastAPI can handle lists of dictionaries
    
    except Exception as e:
        return {"error": str(e)}


def selectunificado():
    try:
        conn=sqlite3.connect('portalcompras.db')
        c=conn.cursor()
        sql="""SELECT id,handle,produtor_relatorio,id_grupo,nome_grupo FROM unificado"""
        c.execute(sql)
        rows = c.fetchall()
        columns = [column[0] for column in c.description]
        conn.close()
        
        # Create DataFrame
        df = pd.DataFrame.from_records(rows, columns=columns)
        
        # Convert to list of dictionaries and ensure native data types
        result = df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")
        
        return result  # FastAPI can handle lists of dictionaries
    
    except Exception as e:
        return {"error": str(e)}

        
        
def deleteunificado(id):
    try:
        conn=sqlite3.connect('portalcompras.db')
        c=conn.cursor()
        c.execute(f'DELETE FROM unificado WHERE id={id}')
        conn.commit()
        conn.close()
        return json.dumps({"message":"Deletado com sucesso !!"})
    except Exception as e:
        return json.dumps({'Error':e})