import os
import shutil
import yaml

# Função para carregar o arquivo .yaml
def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

# Função para verificar se um arquivo/pasta deve ser ignorado
def should_ignore(path, ignore_patterns):
    from fnmatch import fnmatch
    for pattern in ignore_patterns:
        if fnmatch(path, pattern):
            return True
    return False

# Função para copiar os arquivos necessários
def copy_files(src, dest, ignore_patterns):
    if not os.path.exists(dest):
        os.makedirs(dest)

    for root, dirs, files in os.walk(src):
        # Relativo à pasta src
        rel_path = os.path.relpath(root, src)
        
        # Ignorar diretórios
        dirs[:] = [d for d in dirs if not should_ignore(d, ignore_patterns)]

        # Criar a pasta no destino
        dest_dir = os.path.join(dest, rel_path)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        print(f"\n📂 Processando pasta: {root}")

        # Copiar arquivos
        for file in files:
            if not should_ignore(file, ignore_patterns):
                src_file = os.path.join(root, file)
                dest_file = os.path.join(dest_dir, file)
                shutil.copy2(src_file, dest_file)
                print(f"✅ Arquivo copiado: {src_file} -> {dest_file}")
            else:
                print(f"❌ Arquivo ignorado: {file}")

# Caminhos e inicialização
if __name__ == "__main__":
    # Diretório atual (onde está o builder.py)
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    SRC_DIR = CURRENT_DIR
    DEST_DIR = os.path.join(CURRENT_DIR, "build")  # Cria a pasta "build" no mesmo local
    CONFIG_PATH = os.path.join(CURRENT_DIR, "config.yml")

    print("🔧 Carregando configuração...")
    # Carregar configuração
    config = load_config(CONFIG_PATH)
    ignore_patterns = config.get("ignore", [])

    # Iniciar o processo de build
    print(f"🔨 Iniciando o build...\nOrigem: {SRC_DIR}\nDestino: {DEST_DIR}\n")
    copy_files(SRC_DIR, DEST_DIR, ignore_patterns)
    print(f"\n🏁 Build concluído! Arquivos salvos em {DEST_DIR}")
