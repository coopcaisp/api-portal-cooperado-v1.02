from fastapi import FastAPI,Response,HTTPException,UploadFile,File,Request,WebSocket
from typing import Optional
from main import RelatorioPrevia,generate_pdf,RelatorioFinalSemana,generate_pdf_final_de_semana,Grupos,ProdutorPrevia,Produtores
from fastapi.responses import StreamingResponse
import re
import os
from starlette.responses import JSONResponse
import zipfile
from io import BytesIO
from datetime import datetime
import json
import pika
import time
import urllib.parse
import requests
import base64
from fastapi.middleware.cors import CORSMiddleware
from pdf2image import convert_from_bytes
from msg.db import criar_db,cadastrar_msg,criardb_gp,delete_msg,update_gp,inserir_grupo,delete_gp,update_msg,SelectGrupo,droptable,SelecionarMensagem,InserirMensagemPadrao,SelecionarMensagemPadrao,UpdateMensagemPadrao,insertunificado,selectunificado,deleteunificado,SelectProdutor,DeletarMensagemPadrao
from pydantic import BaseModel 
import asyncio




app=FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite apenas as origens listadas
    allow_credentials=True,  # Habilita o envio de cookies e credenciais
    allow_methods=["*"],     # Permite todos os métodos (GET, POST, etc.)
    allow_headers=["*"],     # Permite todos os headers
)
class Confirmacao(BaseModel):
    numero: int
    mensagem: str
    filename: str
    status: str
    

# Lista de conexões WebSocket ativas
active_connections = []

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_message(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            print(f"Mensagem recebida do cliente: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("Cliente desconectado")



# Webhook para receber atualizações de status
@app.post("/api/v1/webhook_status")
async def webhook_status(request: Request):
    """
    Recebe logs enviados pelo consumidor e retorna os dados recebidos.
    """
    try:
        # Obtém os dados do corpo da requisição como JSON
        log = await request.json()
        
        # Verifica se o corpo está vazio
        if not log:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": "Nenhum dado foi enviado no corpo da requisição."
                }
            )

        # Exibe no console para debug
        print(f"Log recebido: {log}")
        
        # Retorna os dados exatamente como foram enviados
        return {
            "status": "success",
            "message": "Log recebido e processado com sucesso!",
            "log": log  # Retorna os dados recebidos
        }
    except ValueError:
        # Caso o JSON seja inválido
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "message": "O corpo da requisição não contém um JSON válido."
            }
        )
    except Exception as e:
        # Retorna erro genérico em caso de falha
        return {
            "status": "error",
            "message": f"Erro ao processar log: {str(e)}"
        }


@app.post("/api/v1/confirmacao")
async def confirmacao(confirmacao: Confirmacao):
    # Processa a confirmação recebida
    print("Confirmação recebida:", confirmacao)
    return {"status": "sucesso", "detalhes": f"Mensagem para {confirmacao.numero} confirmada com sucesso"}
# Função para enviar o PDF via proxy
def enviar_pdf_e_mensagem(pdf_content: bytes, filename: str, numero: str, mensagem: str = ""):
    # Codificar o PDF em base64
    pdf_base64 = base64.b64encode(pdf_content).decode('utf-8')
    
    # Estrutura da mensagem
    payload = {
        "numero": numero,
        "mensagem": mensagem,
        "pdf": {
            "filename": filename,
            "data": pdf_base64,
            "mimeType": "application/pdf"
        }
    }
    
    # Conectar ao RabbitMQ e enviar a mensagem para a fila
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Declara a fila com persistência
        channel.queue_declare(queue='fila_envio_mensagem', durable=True)

        # Publica a mensagem na fila
        channel.basic_publish(
            exchange='',
            routing_key='fila_envio_mensagem',
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Mensagem persistente
            )
        )
        print("Mensagem enfileirada para envio.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao enfileirar a mensagem: {str(e)}")
    finally:
        if 'connection' in locals() and connection.is_open:
            connection.close()
    
    return {"status": "Mensagem enfileirada com sucesso"}

def aceitar_convite(invite_code: str):
    url = "http://192.168.1.61:3000/client/acceptInvite/botcaisp"
    headers = {
        'accept': '*/*',
        'x-api-key': 'bot_caisp',
        'Content-Type': 'application/json',
    }
    payload = {
        "inviteCode": invite_code
    }

    # Enviando o código de convite para o serviço
    response = requests.post(url, headers=headers, json=payload)

    # Verificando a resposta
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"Erro ao aceitar convite: {response.text}")

    # Retornando o JSON da resposta
    return response.json()


@app.post("/api/v1/aceitar_convite/")
async def aceitar_convite_api(invite_code: str):
    # Chama a função para enviar o código de convite para a API
    try:
        response = aceitar_convite(invite_code)
        if response.get("success"):
            return {"message": "Convite aceito com sucesso.", "acceptInviteId": response["acceptInvite"]}
        else:
            raise HTTPException(status_code=500, detail="Falha ao aceitar o convite.")
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/enviandorelatorio/")
async def enviando_mensagem_produtores(numero: str, mensagem: str, grupo: int, produtor: str):
    # Decodificar valores da URL
    numero = urllib.parse.unquote(numero)
    produtor = urllib.parse.unquote(produtor)
    
    # Obter dados para o relatório
    df = RelatorioPrevia(grupo, produtor)
    if df.empty:
        return {"message": "Nenhum dado foi retornado da consulta."}

    # Nome seguro para o arquivo
    safe_filename = re.sub(r'[^a-zA-Z0-9]', '_', str(produtor))
    filename = f"relatorio_{safe_filename}.pdf"
    
    # Criação do PDF em memória
    pdf_buffer = BytesIO()
    data_pedido = df['DATA DO PEDIDO'].iloc[0] if 'DATA DO PEDIDO' in df.columns else "Data não encontrada"
    generate_pdf(df, str(produtor), pdf_buffer, grupo, data_pedido)
    pdf_buffer.seek(0)  # Retorna ao início do buffer para leitura
    pdf_content = pdf_buffer.getvalue()  # Obter o conteúdo do PDF
    
    # Envio do PDF e da mensagem
    try:
        response = enviar_pdf_e_mensagem(pdf_content, filename, numero, mensagem)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=f"Erro ao enviar: {e.detail}")

    # Retornar confirmação de enfileiramento
    return {"message": "PDF e mensagem enfileirados com sucesso. O envio será processado em breve."}

@app.get("/api/v1/relatorioprevia/")
def Relatorio_de_previa(grupo: int, produtor: Optional[str] = None):
    df = RelatorioPrevia(grupo, produtor)
    if produtor:
        # Extrair a data do pedido do primeiro registro (se disponível)
        data_pedido = df['DATA DO PEDIDO'].iloc[0] if 'DATA DO PEDIDO' in df.columns else "Data não encontrada"
        
   
    if df.empty:
        return {"message": "Nenhum dado foi retornado da consulta."}

    if produtor:
        # Nome seguro para o arquivo
        safe_filename = re.sub(r'[^a-zA-Z0-9]', '_', produtor)
        filename = f"relatorio_{safe_filename}.pdf"
        
        # Criação de buffer em memória
        pdf_buffer = BytesIO()
        
        # Gerar o PDF no buffer
        generate_pdf(df, produtor, pdf_buffer, grupo, data_pedido)
        
        # Retornar o PDF como StreamingResponse
        pdf_buffer.seek(0)  # Retorna ao início do buffer para leitura
        return StreamingResponse(
            pdf_buffer,
            media_type='application/pdf',
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    else:
        # Se o produtor não for fornecido, gera PDFs para todos e os compacta em um ZIP
        pdf_files = []
        zip_filename = "relatorios.zip"
        zip_buffer = BytesIO()

        # Gerar PDFs separados para cada fornecedor e adicioná-los ao ZIP
        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED) as zip_file:
            for fornecedor in df['NOME FORNECEDOR'].unique():
                fornecedor_df = df[df['NOME FORNECEDOR'] == fornecedor]
                
                # Extrair a data do pedido do primeiro registro para cada fornecedor
                data_pedido = fornecedor_df['DATA DO PEDIDO'].iloc[0] if 'DATA DO PEDIDO' in fornecedor_df.columns else "Data não encontrada"
                
                # Nome do arquivo baseado no nome do fornecedor, sem caracteres especiais
                safe_filename = re.sub(r'[^a-zA-Z0-9]', '_', fornecedor)
                filename = f"relatorio_{safe_filename}.pdf"
                
                # Gerar o PDF com data_pedido incluído
                generate_pdf(fornecedor_df, fornecedor, filename, grupo, data_pedido)
                
                # Adiciona o PDF ao arquivo ZIP
                zip_file.write(filename)
                pdf_files.append(filename)

        # Retorna o conteúdo do ZIP
        zip_buffer.seek(0)
        response = Response(content=zip_buffer.getvalue(), media_type="application/zip")
        response.headers["Content-Disposition"] = f"attachment; filename={zip_filename}"
        
        time.sleep(1)
        # Remover arquivos PDF após criar o ZIP
        for pdf_file in pdf_files:
            os.remove(pdf_file)
        
        return response

    
    
@app.get("/api/v1/relatoriofinaldesemana/")
def Relatorio_de_previa_final_de_semana(grupo,produtor: Optional[str] = None):
    df = RelatorioFinalSemana(grupo,produtor)

    if df.empty:
        return {"message": "Nenhum dado foi retornado da consulta."}

    if produtor:
        # Nome seguro para o arquivo
        safe_filename = re.sub(r'[^a-zA-Z0-9]', '_', produtor)
        filename = f"relatorio_{safe_filename}.pdf"
        
        # Criação de buffer em memória
        pdf_buffer = BytesIO()
        
        # Gerar o PDF no buffer
        generate_pdf_final_de_semana(df, produtor, pdf_buffer)
        
        # Retornar o PDF como StreamingResponse
        pdf_buffer.seek(0)  # Retorna ao início do buffer para leitura
        return StreamingResponse(
            pdf_buffer,
            media_type='application/pdf',
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    else:
        # Se o produtor não for fornecido, gera PDFs para todos e os compacta em um ZIP
        pdf_files = []
        zip_filename = "relatorios.zip"
        zip_buffer = BytesIO()

        # Gerar PDFs separados para cada fornecedor e adicioná-los ao ZIP
        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED) as zip_file:
            for fornecedor in df['PRODUTOR'].unique():
                fornecedor_df = df[df['PRODUTOR'] == fornecedor]
                
                # Nome do arquivo baseado no nome do fornecedor, sem caracteres especiais
                safe_filename = re.sub(r'[^a-zA-Z0-9]', '_', fornecedor)
                filename = f"relatorio_{safe_filename}.pdf"
                generate_pdf_final_de_semana(fornecedor_df, fornecedor, filename)
                
                # Adiciona o PDF ao arquivo ZIP
                zip_file.write(filename)
                pdf_files.append(filename)

        # Retorna o conteúdo do ZIP
        zip_buffer.seek(0)
        response = Response(content=zip_buffer.getvalue(), media_type="application/zip")
        response.headers["Content-Disposition"] = f"attachment; filename={zip_filename}"
        
        # Remover arquivos PDF após criar o ZIP
        for pdf_file in pdf_files:
            os.remove(pdf_file)
        
        return response
    
@app.get("/api/v1/grupo")
def selecionar_grupo():
    select_grupo=Grupos()
    return select_grupo

@app.get("/api/v1/produtor")
def Selecionar_Produtor(grupo:int):
    json_produtor=ProdutorPrevia(grupo)
    return json_produtor


@app.get("/api/v1/viewrelatorioprevia")
async def relatorio_de_previa(grupo: int, produtor: Optional[str] = None):
    df = RelatorioPrevia(grupo, produtor)
    if produtor:
        # Extrair a data do pedido do primeiro registro (se disponível)
        data_pedido = df['DATA DO PEDIDO'].iloc[0] if 'DATA DO PEDIDO' in df.columns else "Data não encontrada"
        

    if df.empty:
        return {"message": "Nenhum dado foi retornado da consulta."}

    # Nome seguro para o arquivo
    safe_filename = re.sub(r'[^a-zA-Z0-9]', '_', produtor or "relatorio")
    
    # Criação de buffer em memória
    pdf_buffer = BytesIO()
    
    # Gerar o PDF no buffer
    generate_pdf(df, produtor, pdf_buffer, grupo,data_pedido)
    
    # Converter o PDF em imagem (primeira página como exemplo)
    pdf_buffer.seek(0)  # Retorna ao início do buffer para leitura
    images = convert_from_bytes(pdf_buffer.getvalue())
    
    # Converter a primeira página do PDF em uma imagem
    img_buffer = BytesIO()
    images[0].save(img_buffer, format="PNG")
    img_buffer.seek(0)

    return StreamingResponse(
        img_buffer,
        media_type="image/png",
        headers={
            "Content-Disposition": f"inline; filename={safe_filename}.png"
        }
    )
@app.get("/api/v1/viewpedidofechado")
async def visualizar_pedido_fechado(grupo: int, produtor: str):
    # Obter os dados
    df = Relatorio_de_previa_final_de_semana(grupo, produtor)
    
    # Verificar se o DataFrame é None ou está vazio
    if df is None or df.empty:
        return {"message": "Nenhum dado foi retornado da consulta."}

    # Extrair 'DATA' do DataFrame para usar como 'data_pedido' se disponível
    data_pedido = df['DATA'].iloc[0] if 'DATA' in df.columns else "Data não encontrada"
    
    # Nome seguro para o arquivo
    safe_filename = re.sub(r'[^a-zA-Z0-9]', '_', produtor or "relatorio")

    # Criar buffer de PDF na memória
    pdf_buffer = BytesIO()

    # Gerar o PDF com data_pedido
    generate_pdf_final_de_semana(df, fornecedor=produtor, filename=pdf_buffer, data_pedido=data_pedido)
    
    # Retornar ao início do buffer
    pdf_buffer.seek(0)

    # Converter o PDF para imagem (primeira página como exemplo)
    images = convert_from_bytes(pdf_buffer.getvalue())
    img_buffer = BytesIO()
    images[0].save(img_buffer, format="PNG")
    img_buffer.seek(0)

    return StreamingResponse(
        img_buffer,
        media_type="image/png",
        headers={
            "Content-Disposition": f"inline; filename={safe_filename}.png"
        }
    )

@app.get("/api/v1/listadeprodutores")
def lista_produtores():
    listprod=Produtores()
    return listprod


    
@app.delete('/api/v1/droptable')
def deletar_tabela():
    excluir_tabela=droptable()
    return excluir_tabela
    
@app.post('/api/v1/criar_db')
def criar_banco_mensagem():
    bancodb=criar_db()
    return bancodb

@app.post("/api/v1/cadastrarmensagem")
def cadastrarmensagem(conteudo:str,id:int):
    cadastrar=cadastrar_msg(conteudo,id)
    return cadastrar

@app.delete("/api/v1/deletemensagem")
def delete_mensagem(id:int):
    delete=delete_msg(id)
    return delete

@app.put("/api/v1/updatemsg")
def updatemensagem(id:int,mensagem:str):
    update=update_msg(id,mensagem)
    return update

@app.get('/api/v1/mensagem')
def VisualizarMensagemPredifinidas():
    mensagem=SelecionarMensagem()
    return mensagem
    

@app.delete("/api/v1/deletargrupo")
def Deletar_grupo(id:int):
    deletar_grupo=delete_gp(id)
    return deletar_grupo


@app.post('/api/v1/criardbgrupo')
def criar_dbgrupo():
    criar_grupo_dbwhats=criardb_gp()
    return criar_grupo_dbwhats

@app.post('/api/v1/cadastrargrupo')
def cadastro_de_grupo(id_grupo:str,nome_grupo:str, descricao: Optional[str] = None):
    cadastrargrupodowhats=inserir_grupo(id_grupo,nome_grupo,descricao)
    return cadastrargrupodowhats

@app.get('/api/v1/gruposdowhats')
def trazer_informacao_dos_grupos_whats():
    selecionarGrupos=SelectGrupo()
    return selecionarGrupos

@app.post('/api/v1/mensagempadrao')
def inserir_msg_padrao(mensagem:str):
    MensagemPadrao=InserirMensagemPadrao(mensagem)
    return MensagemPadrao
@app.get('/api/v1/selecionarmsgpadrao')
def Selecionar_Mensagem_Padrao_pra_todos_os_produtores():
    msgpdradrao=SelecionarMensagemPadrao()
    return msgpdradrao
@app.put('/api/v1/updatemsgpadrao')
def update_mensagem_padrao(id:int,mensagem:str):
    upmensagempadrao=UpdateMensagemPadrao(id,mensagem)
    return upmensagempadrao
@app.delete('/api/v1/deletmsgpadrao')
def deletar_mensagem_padrao(id:int):
    delet=DeletarMensagemPadrao(id)
    return delet

@app.post('/api/v1/vincular')
def vincular_grupo_com_relatorio(handle:int,nomedorelatorio:str,id_grupo:str,grupo:str):
    insert_de_vincular=insertunificado(handle,nomedorelatorio,id_grupo,grupo)
    return insert_de_vincular

@app.get('/api/v1/tabelaunificado')
def tabela_final():
    table=selectunificado()
    return table

@app.get('/api/v1/getprodutor')
def tabela_por_produtor(produtor:str):
    table=SelectProdutor(produtor)
    return table

@app.delete("/api/v1/deletar_unificado")
def tabela_unificado_delet(id:int):
    deletado=deleteunificado(id)
    return deletado