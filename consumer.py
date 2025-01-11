import pika
import requests
import json
import base64
from time import sleep
from io import BytesIO
from urllib.parse import unquote
import sqlite3
import logging
import json 

CONFIG=None
def load_global_config(config_path="config.json"):
    global CONFIG
    with open(config_path,"r") as file:
        CONFIG=json.load(file)
        
load_global_config()

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("consumer.log"),  # Salva logs em arquivo
        logging.StreamHandler()              # Mostra logs no console
    ]
)

# URL do WebSocket

WEBSOCKET_URL = "ws://localhost:8000/ws"

def enviar_notificacao_websocket(message):
    """
    Envia uma notificação para o WebSocket.
    """
    import websockets
    import asyncio

    async def send_message():
        try:
            async with websockets.connect(WEBSOCKET_URL) as websocket:
                await websocket.send(message)
                logging.info(f"Mensagem enviada ao WebSocket: {message}")
        except Exception as e:
            logging.error(f"Erro ao enviar mensagem ao WebSocket: {e}")

    asyncio.run(send_message())

# Função para processar mensagens da fila
def process_message(ch, method, properties, body):
    try:
        logging.info("Mensagem recebida da fila.")
        
        # Decodificar a mensagem recebida
        message = json.loads(body.decode())
        numero = message['numero']
        mensagem = message['mensagem']
        pdf_base64 = message['pdf']['data']
        filename = message['pdf']['filename']
        mimeType = message['pdf']['mimeType']

        logging.info(f"Processando mensagem para o número: {numero}")

        # Decodificar o número da URL (remover %40 para @)
        numero = unquote(numero)

        # URL e cabeçalhos de envio
        urlmsg=CONFIG['urlapi']['urlapiwhats']
        url_envio_mensagem = urlmsg
        headers = {
            'accept': '*/*',
            'x-api-key': 'bot_caisp',
            'Content-Type': 'application/json',
        }

        # Enviar a mensagem de texto
        payload_mensagem = {
            "chatId": numero,
            "contentType": "string",
            "content": mensagem
        }
        response_mensagem = requests.post(url_envio_mensagem, headers=headers, json=payload_mensagem)
        if response_mensagem.status_code != 200:
            raise Exception(f"Erro ao enviar a mensagem: {response_mensagem.text}")

        logging.info(f"Mensagem de texto enviada para o número: {numero}")

        # Enviar o PDF
        payload_pdf = {
            "chatId": numero,
            "contentType": "MessageMedia",
            "content": {
                "mimeType": mimeType,
                "data": pdf_base64,
                "filename": filename
            }
        }
        response_pdf = requests.post(url_envio_mensagem, headers=headers, json=payload_pdf)
        if response_pdf.status_code != 200:
            raise Exception(f"Erro ao enviar o PDF: {response_pdf.text}")

        logging.info(f"PDF enviado para o número: {numero}")

        # Confirma que a mensagem foi processada com sucesso
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info(f"Mensagem e PDF confirmados para o número: {numero}")

        # Confirmar o envio em outra API
        jsonurl=CONFIG['urlapi']['urlconfirmacao']
        url_confirmacao = 'http://localhost:8000/api/v1/confirmacao'
        payload_confirmacao = {
            "numero": numero,
            "mensagem": mensagem,
            "filename": filename,
            "status": "sucesso"
        }
        resposta = requests.post(url_confirmacao, json=payload_confirmacao)
        if resposta.status_code == 200:
            logging.info(f"Confirmação enviada para o número: {numero}")

            # Enviar notificação para o WebSocket
            enviar_notificacao_websocket(json.dumps({
                "numero": numero,
                "mensagem": mensagem,
                "status": "sucesso"
            }))

            # Salvar log no banco SQLite
            with sqlite3.connect('portalcompras.db') as con:
                c = con.cursor()
                c.execute("""INSERT INTO lograbbitmq (Numero_grupo_enviado, mensagem, data_de_envio) 
                             VALUES (?, ?, datetime('now'))""", (numero, mensagem))
                con.commit()
                logging.info(f"Log salvo no banco SQLite para o número: {numero}")

    except Exception as e:
        logging.error(f"Erro ao processar a mensagem: {e}")
        sleep(5)  # Esperar antes de re-enfileirar
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

# Função para iniciar o consumidor
def start_consumer():
    try:
        logging.info("Iniciando consumidor RabbitMQ...")
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Declarar a fila
        channel.queue_declare(queue='fila_envio_mensagem', durable=True)
        logging.info("Conexão com RabbitMQ estabelecida. Consumidor pronto para processar mensagens.")

        # Configurar o consumidor
        channel.basic_qos(prefetch_count=1)  # Processa uma mensagem por vez
        channel.basic_consume(queue='fila_envio_mensagem', on_message_callback=process_message)

        logging.info("Consumidor aguardando mensagens na fila 'fila_envio_mensagem'...")
        channel.start_consuming()
    except Exception as e:
        logging.error(f"Erro ao iniciar o consumidor RabbitMQ: {e}")
        sleep(5)  # Aguarda antes de tentar reconectar
        start_consumer()  # Tenta reiniciar o consumidor

# Executar o consumidor
if __name__ == "__main__":
    start_consumer()
