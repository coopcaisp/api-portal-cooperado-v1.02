import pyodbc
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4,landscape
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Image, Spacer
import sqlite3
from fastapi.responses import JSONResponse
from datetime import datetime
import json

CONFIG=None
def load_global_config(config_path="config.json"):
    global CONFIG
    with open(config_path,"r") as file:
        CONFIG=json.load(file)
        
load_global_config()

def RelatorioFinalSemana(grupo,parametro=None):
    global CONFIG 
    db_config=CONFIG["database"]
    
    conn = pyodbc.connect(
        f"DRIVER={db_config['DRIVER']};SERVER={db_config['SERVER']};"
        f"DATABASE={db_config['DBNAME']};UID={db_config['UID']};PWD={db_config['PWD']}"
    )
    cursor = conn.cursor()
    
    if parametro is not None :
        produtor=f"AND  B.APELIDO ='{parametro}'"
    else:
        produtor="-- AND  B.APELIDO"
    
    # Query SQL
    query = f"""
          SELECT 
        B.APELIDO AS 'PRODUTOR',
        E.DATADOPEDIDO 'DATA',
        D.CODIGOREFERENCIA AS 'COD',
        D.NOME AS 'PRODUTO',
        CM_UNIDADESMEDIDA.ABREVIATURA AS 'MEDIDA',
        (A.QUANTIDADEPREVIA + A.QUANTIDADEAJUSTADA) AS 'TOTAL',
        PD_PRODUTOS2.NOME AS 'TIPO DE CAIXA',
		K_PD_GRUPOSGRUPODEPRODUTO.NOME AS 'GRUPO'
    FROM K_CM_CORTEPRODUTOFORNECEDORES AS A
    LEFT OUTER JOIN GN_PESSOAS AS B ON (B.HANDLE = A.PESSOA)
    LEFT OUTER JOIN K_CM_CORTEPRODUTOS AS C ON (C.HANDLE = A.CORTEPRODUTO)
    LEFT OUTER JOIN PD_PRODUTOS AS D ON (D.HANDLE = C.PRODUTO)
    LEFT OUTER JOIN K_CM_CORTES AS E ON (E.HANDLE = C.CORTE)
    LEFT OUTER JOIN PD_GRUPOSPRODUTOS ON (PD_GRUPOSPRODUTOS.HANDLE = D.GRUPO)
    LEFT OUTER JOIN K_PD_GRUPOSGRUPODEPRODUTO ON (K_PD_GRUPOSGRUPODEPRODUTO.HANDLE = PD_GRUPOSPRODUTOS.K_GRUPO)
    LEFT OUTER JOIN CM_UNIDADESMEDIDA ON (CM_UNIDADESMEDIDA.HANDLE = D.UNIDADEMEDIDACOMPRAS)
    LEFT OUTER JOIN PD_PRODUTOS AS PD_PRODUTOS2 ON (PD_PRODUTOS2.HANDLE = D.K_PRODUTOCAIXA)
    LEFT OUTER JOIN CP_ORDENSCOMPRAITENS ON (CP_ORDENSCOMPRAITENS.K_CORTEPRODUTOFORNECEDOR = A.HANDLE OR CP_ORDENSCOMPRAITENS.K_PREVIAPRODUTOFORNECEDOR = A.PREVIAPRODUTOFORNECEDOR)
    LEFT OUTER JOIN CP_ORDENSCOMPRA ON (CP_ORDENSCOMPRA.HANDLE = CP_ORDENSCOMPRAITENS.ORDEMCOMPRA)
    WHERE E.DATADOPEDIDO =CONVERT(DATE, DATEADD(DAY, 1, GETDATE()))
    AND E.STATUS <> 5
    AND B.CODIGO NOT IN (550)
    AND (A.QUANTIDADEPREVIA + A.QUANTIDADEAJUSTADA) > 0
    AND K_PD_GRUPOSGRUPODEPRODUTO.HANDLE={grupo}
    {produtor}
    
   ORDER BY GRUPO ASC , PRODUTO ASC ,PRODUTOR ASC
   
    """
    
    # Executar a consulta SQL
    cursor.execute(query)
    rows = cursor.fetchall()

    # Fechar conexão
    cursor.close()
    conn.close()

    # Verificar se há dados retornados
    if not rows:
        return pd.DataFrame()  # Retorna DataFrame vazio se não houver resultados

    # Obter nomes das colunas
    columns = [column[0] for column in cursor.description]
    
    # Converter resultados para DataFrame
    df = pd.DataFrame.from_records(rows, columns=columns)
    
    return df
    

def RelatorioPrevia(grupo,parametro=None):
    # Estabelecer a conexão com o banco de dados
    global CONFIG 
    db_config=CONFIG["database"]
    conn = pyodbc.connect(
        f"DRIVER={db_config['DRIVER']};SERVER={db_config['SERVER']};"
        f"DATABASE={db_config['DBNAME']};UID={db_config['UID']};PWD={db_config['PWD']}"
    )
    cursor = conn.cursor()
    
    if parametro is not None:
        produtor=f"AND GN_PESSOAS.NOME='{parametro}'"
        
    else:
        produtor="--AND GN_PESSOAS.NOME"
        
        
    
    
    
    # Query SQL
    query = f"""
        
	      SELECT  
    GN_PESSOAS.NOME AS 'NOME FORNECEDOR', 
	 K_CM_PREVIAS.DATADOPEDIDO AS 'DATA DO PEDIDO',
	 PD_PRODUTOS.CODIGOREFERENCIA AS 'CODIGO DO PRODUTO', 
	 PD.NOME AS 'TIPO DE CX',
    PD_PRODUTOS.NOME AS 'NOME DO PRODUTO', 
    CM_UNIDADESMEDIDA.ABREVIATURA AS 'UNIDADE DE MEDIDA', 
    A.QUANTIDADEAJUSTADA AS 'QTD PEDIDA', 
    PD_PRODUTOS.K_QUANTIDADEPORCAIXA AS 'QTD POR CAIXA',
    K_PD_GRUPOSGRUPODEPRODUTO.NOME AS 'GRUPO'
    
   
FROM 
    K_CM_PREVIAPRODUTOFORNECEDORES A 
    LEFT OUTER JOIN GN_PESSOAS GN_PESSOAS 
        ON (GN_PESSOAS.HANDLE = A.PESSOA) 
    LEFT OUTER JOIN K_CM_PREVIAPRODUTOS K_CM_PREVIAPRODUTOS 
        ON (K_CM_PREVIAPRODUTOS.HANDLE = A.PREVIAPRODUTO) 
    LEFT OUTER JOIN PD_PRODUTOS PD_PRODUTOS 
        ON (PD_PRODUTOS.HANDLE = K_CM_PREVIAPRODUTOS.PRODUTO) 
    LEFT OUTER JOIN PD_GRUPOSPRODUTOS PD_GRUPOSPRODUTOS 
        ON (PD_GRUPOSPRODUTOS.HANDLE = PD_PRODUTOS.GRUPO) 
    LEFT OUTER JOIN K_PD_GRUPOSGRUPODEPRODUTO K_PD_GRUPOSGRUPODEPRODUTO 
        ON (K_PD_GRUPOSGRUPODEPRODUTO.HANDLE = PD_GRUPOSPRODUTOS.K_GRUPO) 
    LEFT OUTER JOIN CM_UNIDADESMEDIDA CM_UNIDADESMEDIDA 
        ON (CM_UNIDADESMEDIDA.HANDLE = K_CM_PREVIAPRODUTOS.UNIDADE) 
    LEFT OUTER JOIN PD_PRODUTOS PD 
        ON (PD.HANDLE = PD_PRODUTOS.K_PRODUTOCAIXA) 
    JOIN K_CM_PREVIAS K_CM_PREVIAS 
        ON (K_CM_PREVIAS.HANDLE = K_CM_PREVIAPRODUTOS.PREVIA) 
WHERE  
     K_CM_PREVIAS.DATADOPEDIDO = CONVERT(DATE, DATEADD(DAY, 1, GETDATE()))
	 AND A.QUANTIDADEAJUSTADA > 0  
	 AND K_PD_GRUPOSGRUPODEPRODUTO.HANDLE={grupo}
    {produtor}
	
    

 ORDER BY K_PD_GRUPOSGRUPODEPRODUTO.NOME ASC , [NOME FORNECEDOR] ASC;


    """
    
    # Executar a consulta SQL
    cursor.execute(query)
    rows = cursor.fetchall()

    # Se não houver dados, exibir uma mensagem
    if not rows:
        print("Nenhum dado encontrado.")
        return None
    
    # Obter os nomes das colunas
    columns = [column[0] for column in cursor.description]
    
    # Converter os resultados em DataFrame
    df = pd.DataFrame.from_records(rows, columns=columns)
    
    cursor.close()
    conn.close()
    
    return df

def generate_pdf(dataframe, fornecedor, filename,grupo,data_pedido):
    # Define a margem reduzida para expandir o conteúdo
    doc = SimpleDocTemplate(filename, pagesize=landscape(A4), leftMargin=1*cm, rightMargin=1*cm, topMargin=1*cm, bottomMargin=1*cm)
    elements = []
    
    if isinstance(data_pedido, pd.Timestamp):
        data_pedido = data_pedido.strftime("%Y-%m-%d %H:%M:%S")  # Converte para string no formato desejado inicialmente

    try:
        data_pedido_formatado = datetime.strptime(data_pedido, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y")
    except ValueError:
        data_pedido_formatado = data_pedido # Caso a data já esteja no formato desejado ou ocorra algum erro
    
    # Estilos para o documento
    styles = getSampleStyleSheet()
    title_style = styles['Title']
    title_style.fontSize = 12
    normal_style = styles['Normal']
    
    # Adicionar logo
    logo_path = CONFIG["paths"]["images"]
    img = Image(logo_path, width=5*cm, height=4*cm)
    elements.append(img)
    
    # Título do documento
    title = Paragraph(f"PRÉVIA - {fornecedor}", title_style)
    elements.append(title)
    
    # Informação do produtor
    producer_info = Paragraph(f"<b>Produtor:</b> {fornecedor}", normal_style)
    elements.append(producer_info)
    data_info = Paragraph(f"<b>Data: </b>{data_pedido_formatado}", normal_style)
    elements.append(data_info)


    # Adicionar espaçamento de 1 cm
    elements.append(Spacer(0, 1*cm))
    
    # Criando os dados da tabela
    table_data = [['Código', 'Produto', 'Unidade', 'Prévia', 'Qtde x Caixa', 'Tipo de Caixa', 'Grupo']]
    
    # Preenchendo os dados da tabela
    for i, row in dataframe.iterrows():
        table_data.append([
            row['CODIGO DO PRODUTO'], 
            row['NOME DO PRODUTO'], 
            row['UNIDADE DE MEDIDA'], 
            row['QTD PEDIDA'], 
            row['QTD POR CAIXA'],  
            row['TIPO DE CX'],
            row['GRUPO']
        ])
    
    # Ajustar larguras das colunas para ocupar mais espaço
    col_widths = [3*cm, 9*cm, 2*cm, 2*cm, 3*cm, 4*cm, 6*cm] 
    
    # Estilo da tabela
    table = Table(table_data, colWidths=col_widths, repeatRows=1)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.white),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('LINEBELOW', (0, 0), (-1, -1), 1, colors.black),  # Apenas linhas horizontais
        ('LINEABOVE', (0, 0), (-1, 0), 1, colors.black),
        # Diminui a fonte para a penúltima coluna "Tipo de Caixa" e última coluna "Grupo"
        ('FONTSIZE', (-2, 1), (-1, -1), 7),  # Ajusta o tamanho da fonte para as duas últimas colunas
    ]))
    
    # Adicionando a tabela ao documento
    elements.append(table)
    
    # Construindo o PDF
    doc.build(elements)
    relatorio=f"Relatório gerado para: {fornecedor} - {filename} - {grupo}"
    try:
        
        conn=sqlite3.connect("log.db")
        cursor=conn.cursor()
        query="INSERT INTO log (pdfgerado,data_da_geracao,grupo) VALUES(?,datetime('now'),?)"
        cursor.execute(query,(relatorio,grupo))
        conn.commit()
        print('log criado no banco ')
        conn.close()
    except Exception as err:
        print('erro ao inserir log no banco:', err)
    
def generate_pdf_final_de_semana(dataframe, fornecedor, filename,data_pedido):
    # Define a margem reduzida para expandir o conteúdo
    doc = SimpleDocTemplate(filename, pagesize=landscape(A4), leftMargin=1*cm, rightMargin=1*cm, topMargin=1*cm, bottomMargin=1*cm)
    elements = []
    if isinstance(data_pedido, pd.Timestamp):
        data_pedido = data_pedido.strftime("%Y-%m-%d %H:%M:%S")  # Converte para string no formato desejado inicialmente

    try:
        data_pedido_formatado = datetime.strptime(data_pedido, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y")
    except ValueError:
        data_pedido_formatado = data_pedido # Caso a data já esteja no formato desejado ou ocorra algum erro
    
    # Estilos para o documento
    styles = getSampleStyleSheet()
    title_style = styles['Title']
    title_style.fontSize = 12
    normal_style = styles['Normal']
    
    # Adicionar logo
    img = Image("img/logo.png", width=5*cm, height=4*cm)
    elements.append(img)
    elements.append(img)
    
    # Título do documento
    title = Paragraph(f"PEDIDO FECHADO - {fornecedor}", title_style)
    elements.append(title)
    
    # Informação do produtor
    producer_info = Paragraph(f"<b>Produtor:</b> {fornecedor}", normal_style)
    elements.append(producer_info)
    data_info = Paragraph(f"<b>Data: </b>{data_pedido_formatado}", normal_style)
    elements.append(data_info)

    # Adicionar espaçamento de 1 cm
    elements.append(Spacer(0, 1*cm))
    
    # Criando os dados da tabela
    table_data = [['Código', 'Produto', 'Unidade', 'total', 'Tipo de Caixa', 'Grupo']]
    
    # Preenchendo os dados da tabela
    for i, row in dataframe.iterrows():
        table_data.append([
            row['COD'], 
            row['PRODUTO'], 
            row['MEDIDA'], 
            row['TOTAL'],  
            row['TIPO DE CAIXA'],
            row['GRUPO']
        ])
    
    # Ajustar larguras das colunas para ocupar mais espaço
    col_widths = [3*cm, 9*cm, 2*cm, 2*cm, 3*cm, 4*cm, 6*cm] 
    
    # Estilo da tabela
    table = Table(table_data, colWidths=col_widths, repeatRows=1)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.white),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('LINEBELOW', (0, 0), (-1, -1), 1, colors.black),  # Apenas linhas horizontais
        ('LINEABOVE', (0, 0), (-1, 0), 1, colors.black),
        # Diminui a fonte para a penúltima coluna "Tipo de Caixa" e última coluna "Grupo"
        ('FONTSIZE', (-2, 1), (-1, -1), 7),  # Ajusta o tamanho da fonte para as duas últimas colunas
    ]))
    
    # Adicionando a tabela ao documento
    elements.append(table)
    
    # Construindo o PDF
    doc.build(elements)
    print(f"Relatório gerado para: {fornecedor} - {filename}")




def Grupos():
    try:
        global CONFIG 
        db_config=CONFIG["database"]
        conn = pyodbc.connect(
            f"DRIVER={db_config['DRIVER']};SERVER={db_config['SERVER']};"
            f"DATABASE={db_config['DBNAME']};UID={db_config['UID']};PWD={db_config['PWD']}"
        )
        cursor = conn.cursor()
        query="""SELECT HANDLE,NOME FROM K_PD_GRUPOSGRUPODEPRODUTO"""
        cursor.execute(query)
        rows=cursor.fetchall()
        colunas=[desc[0]for desc in cursor.description]
        rows=[list(row)for row in rows]
        df=pd.DataFrame(rows,columns=colunas)
        resultado=df.to_dict(orient='records')
        cursor.close()
        conn.close()
        return resultado
        
    except Exception as err:
        return JSONResponse(f'Error:{err}')
    
    
def ProdutorPrevia(grupo):
    try:
        global CONFIG 
        db_config=CONFIG["database"]
        conn = pyodbc.connect(
            f"DRIVER={db_config['DRIVER']};SERVER={db_config['SERVER']};"
            f"DATABASE={db_config['DBNAME']};UID={db_config['UID']};PWD={db_config['PWD']}"
        )
        cursor = conn.cursor()
        query=f"""     SELECT  DISTINCT 
    GN_PESSOAS.NOME AS 'NOME FORNECEDOR', 
	 K_CM_PREVIAS.DATADOPEDIDO AS 'DATA DO PEDIDO',
    K_PD_GRUPOSGRUPODEPRODUTO.NOME AS 'GRUPO'
    
   
FROM 
    K_CM_PREVIAPRODUTOFORNECEDORES A 
    LEFT OUTER JOIN GN_PESSOAS GN_PESSOAS 
        ON (GN_PESSOAS.HANDLE = A.PESSOA) 
    LEFT OUTER JOIN K_CM_PREVIAPRODUTOS K_CM_PREVIAPRODUTOS 
        ON (K_CM_PREVIAPRODUTOS.HANDLE = A.PREVIAPRODUTO) 
    LEFT OUTER JOIN PD_PRODUTOS PD_PRODUTOS 
        ON (PD_PRODUTOS.HANDLE = K_CM_PREVIAPRODUTOS.PRODUTO) 
    LEFT OUTER JOIN PD_GRUPOSPRODUTOS PD_GRUPOSPRODUTOS 
        ON (PD_GRUPOSPRODUTOS.HANDLE = PD_PRODUTOS.GRUPO) 
    LEFT OUTER JOIN K_PD_GRUPOSGRUPODEPRODUTO K_PD_GRUPOSGRUPODEPRODUTO 
        ON (K_PD_GRUPOSGRUPODEPRODUTO.HANDLE = PD_GRUPOSPRODUTOS.K_GRUPO) 
    LEFT OUTER JOIN CM_UNIDADESMEDIDA CM_UNIDADESMEDIDA 
        ON (CM_UNIDADESMEDIDA.HANDLE = K_CM_PREVIAPRODUTOS.UNIDADE) 
    LEFT OUTER JOIN PD_PRODUTOS PD 
        ON (PD.HANDLE = PD_PRODUTOS.K_PRODUTOCAIXA) 
    JOIN K_CM_PREVIAS K_CM_PREVIAS 
        ON (K_CM_PREVIAS.HANDLE = K_CM_PREVIAPRODUTOS.PREVIA) 
WHERE  
     K_CM_PREVIAS.DATADOPEDIDO = CONVERT(DATE, DATEADD(DAY, 1, GETDATE()))
	 AND A.QUANTIDADEAJUSTADA > 0  
	 AND K_PD_GRUPOSGRUPODEPRODUTO.HANDLE={grupo}

    

ORDER BY
    K_PD_GRUPOSGRUPODEPRODUTO.NOME ASC;
"""
        cursor.execute(query)
        rows=cursor.fetchall()
        colunas=[desc[0]for desc in cursor.description]
        rows=[list(row)for row in rows]
        df=pd.DataFrame(rows,columns=colunas)
        resultado=df.to_dict(orient='records')
        cursor.close()
        conn.close()
        return resultado
        
    except Exception as err:
        return JSONResponse(f'Error:{err}')
    
    
def Produtores():
    try:
        global CONFIG 
        db_config=CONFIG["database"]
        conn = pyodbc.connect(
            f"DRIVER={db_config['DRIVER']};SERVER={db_config['SERVER']};"
            f"DATABASE={db_config['DBNAME']};UID={db_config['UID']};PWD={db_config['PWD']}"
        )
        cursor = conn.cursor()
        query=f"""SELECT HANDLE,NOME,CODIGO FROM GN_PESSOAS WHERE EHFORNECEDOR='S' AND INATIVO='N' AND EHPRODUTORRURAL='S' ORDER BY NOME ASC """
        cursor.execute(query)
        rows=cursor.fetchall()
        colunas=[desc[0]for desc in cursor.description]
        rows=[list(row)for row in rows]
        df=pd.DataFrame(rows,columns=colunas)
        resultado=df.to_dict(orient='records')
        cursor.close()
        conn.close()
        return resultado
        
    except Exception as err:
        return JSONResponse(f'Error:{err}')