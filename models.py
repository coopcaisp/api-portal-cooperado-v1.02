import pyodbc
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4,landscape
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Image, Spacer
import sqlite3
from fastapi.responses import JSONResponse
import json 

CONFIG=None
def load_global_config(config_path="config.json"):
    global CONFIG
    with open(config_path,"r") as file:
        CONFIG=json.load(file)
        
load_global_config()


def RelatorioFinalSemana(parametro=None):
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
    WHERE E.DATADOPEDIDO BETWEEN '17/10/2024' AND '17/10/2024'
    AND E.STATUS <> 5
    AND B.CODIGO NOT IN (550)
    AND (A.QUANTIDADEPREVIA + A.QUANTIDADEAJUSTADA) > 0
    {produtor}
    
   ORDER BY GRUPO ASC , PRODUTO ASC ,PRODUTOR ASC
   
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
    CP_ORDENSCOMPRA.K_DATADOPEDIDO AS 'DATA DO PEDIDO',
    PD_PRODUTOS.CODIGOREFERENCIA AS 'CODIGO DO PRODUTO',
    P2.NOME AS 'TIPO DE CX',
    PD_PRODUTOS.NOME AS 'NOME DO PRODUTO',
	CM_UNIDADESMEDIDA.ABREVIATURA AS 'UNIDADE DE MEDIDA',
    CP_ORDENSCOMPRAITENS.QUANTIDADE AS 'QTD PEDIDA',
	PD_PRODUTOS.K_QUANTIDADEPORCAIXA AS 'QTD POR CAIXA',
	K_PD_GRUPOSGRUPODEPRODUTO.NOME AS 'GRUPO'
   
    
FROM
    CP_ORDENSCOMPRAITENS
    LEFT OUTER JOIN CP_ORDENSCOMPRA ON CP_ORDENSCOMPRA.HANDLE = CP_ORDENSCOMPRAITENS.ORDEMCOMPRA
    LEFT OUTER JOIN GN_PESSOAS ON GN_PESSOAS.HANDLE = CP_ORDENSCOMPRA.FORNECEDOR
    LEFT OUTER JOIN PD_PRODUTOS ON PD_PRODUTOS.HANDLE = CP_ORDENSCOMPRAITENS.PRODUTO
    LEFT OUTER JOIN PD_PRODUTOS AS P2 ON P2.HANDLE = PD_PRODUTOS.K_PRODUTOCAIXA
    LEFT OUTER JOIN CM_UNIDADESMEDIDA ON CM_UNIDADESMEDIDA.HANDLE = CP_ORDENSCOMPRAITENS.UNIDADE
	LEFT OUTER JOIN PD_GRUPOSPRODUTOS ON (PD_GRUPOSPRODUTOS.HANDLE = PD_PRODUTOS.GRUPO)
    LEFT OUTER JOIN K_PD_GRUPOSGRUPODEPRODUTO ON (K_PD_GRUPOSGRUPODEPRODUTO.HANDLE = PD_GRUPOSPRODUTOS.K_GRUPO)
WHERE
    CP_ORDENSCOMPRA.K_DATADOPEDIDO BETWEEN CONVERT(DATETIME, '17/10/2024', 103) AND CONVERT(DATETIME, '17/10/2024', 103)
    AND CP_ORDENSCOMPRA.USUARIOINCLUIU <> 62
    AND K_PD_GRUPOSGRUPODEPRODUTO.HANDLE={grupo}
    {produtor}
	
ORDER BY
    K_PD_GRUPOSGRUPODEPRODUTO.NOME ASC , [NOME DO PRODUTO] ASC;
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

def generate_pdf(dataframe, fornecedor, filename,grupo):
    # Define a margem reduzida para expandir o conteúdo
    doc = SimpleDocTemplate(filename, pagesize=landscape(A4), leftMargin=1*cm, rightMargin=1*cm, topMargin=1*cm, bottomMargin=1*cm)
    elements = []
    
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
    
def generate_pdf_final_de_semana(dataframe, fornecedor, filename):
    # Define a margem reduzida para expandir o conteúdo
    doc = SimpleDocTemplate(filename, pagesize=landscape(A4), leftMargin=1*cm, rightMargin=1*cm, topMargin=1*cm, bottomMargin=1*cm)
    elements = []
    
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
    title = Paragraph(f"PEDIDO FECHADO - {fornecedor}", title_style)
    elements.append(title)
    
    # Informação do produtor
    producer_info = Paragraph(f"<b>Produtor:</b> {fornecedor}", normal_style)
    elements.append(producer_info)

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