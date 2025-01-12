#!/bin/bash

# Atualizar pacotes e instalar dependências básicas
echo "Atualizando pacotes e instalando dependências..."
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv curl wget

# Instalar o driver ODBC 2017 para SQL Server
echo "Instalando o driver ODBC 2017 para SQL Server..."
if ! command -v odbcinst &> /dev/null
then
    sudo apt install -y odbcinst odbcinst1debian2 unixodbc
fi
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
sudo curl -o /etc/apt/sources.list.d/mssql-release.list https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list
sudo apt update
ACCEPT_EULA=Y sudo apt install -y msodbcsql17

# Baixar e instalar o RabbitMQ
if ! command -v rabbitmqctl &> /dev/null
then
    echo "Instalando RabbitMQ..."
    sudo apt install -y erlang erlang-nox
    wget -O rabbitmq.deb https://github.com/rabbitmq/rabbitmq-server/releases/download/v3.12.4/rabbitmq-server_3.12.4-1_all.deb
    sudo dpkg -i rabbitmq.deb
    rm rabbitmq.deb
    sudo systemctl enable rabbitmq-server
    sudo systemctl start rabbitmq-server
else
    echo "RabbitMQ já está instalado."
fi

# Criar ambiente virtual
echo "Criando ambiente virtual..."
python3 -m venv venv
source venv/bin/activate

# Instalar pacotes Python necessários
echo "Instalando dependências Python no ambiente virtual..."
pip install --upgrade pip
pip install -r requirements.txt

deactivate

# Criar serviços para a API e o Consumer
API_SERVICE="/etc/systemd/system/api.service"
CONSUMER_SERVICE="/etc/systemd/system/consumer.service"

# Serviço da API
echo "Criando serviço para a API..."
sudo bash -c "cat > $API_SERVICE" <<EOL
[Unit]
Description=API Service
After=network.target

[Service]
User=$(whoami)
WorkingDirectory=$(pwd)
ExecStart=$(pwd)/venv/bin/python3 $(pwd)/api.py
Restart=always

[Install]
WantedBy=multi-user.target
EOL

# Serviço do Consumer
echo "Criando serviço para o Consumer..."
sudo bash -c "cat > $CONSUMER_SERVICE" <<EOL
[Unit]
Description=Consumer Service
After=rabbitmq-server.service

[Service]
User=$(whoami)
WorkingDirectory=$(pwd)
ExecStart=$(pwd)/venv/bin/python3 $(pwd)/consumer.py
Restart=always

[Install]
WantedBy=multi-user.target
EOL

# Recarregar e habilitar os serviços
echo "Recarregando daemon do systemd e habilitando serviços..."
sudo systemctl daemon-reload
sudo systemctl enable api.service
sudo systemctl enable consumer.service

# Iniciar os serviços
echo "Iniciando serviços..."
sudo systemctl start api.service
sudo systemctl start consumer.service

echo "Instalação concluída e serviços configurados com sucesso!"
