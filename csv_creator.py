import pymssql
import pandas as pd
from ftplib import FTP
from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)
simplefilter(action='ignore', category=Warning)

def conn_pymssql():
    
    # Parametros do banco de dados
    server = '192.168.48.9'
    database = '009JV'
    username = 'usr_pb'
    password = 'pb@18!*'

    # Criar Conexão com banco de dados
    conn = pymssql.connect(
        server, username, password, database
    )
    return conn

def conn_cursor():
    conn = conn_pymssql()
    cursor = conn.cursor()
    return cursor

# Query clientes
def queryClientes():
    query = '''SELECT
    RTRIM( a.iclis) AS "Cod. Cliente",
    RTRIM( a.rclis) AS "Nome",
    RTRIM( a.cpfs) AS "CPF",
    CONVERT(VARCHAR(10), a.nascs, 23) AS "Data Nascimento",
    CONVERT(VARCHAR(10), a.dtcasas, 23) AS "Data Casamento",
    RTRIM( a.ddds) AS "DDD",
    CASE
        WHEN RTRIM(a.faxs) = ''
        AND RTRIM(a.tel1s) = ''
        AND RTRIM(a.tel2s) = '' THEN RTRIM(a.tel3s)
        
        WHEN RTRIM(a.faxs) = ''
        AND RTRIM(a.tel1s) = '' THEN RTRIM(a.tel2s)
        
        WHEN RTRIM(a.faxs) = '' 
        AND RTRIM(a.tel1s) <> '' THEN RTRIM(a.tel1s)
        ELSE RTRIM(a.faxs)
    END AS "Telefone",
    RTRIM( a.emails) AS "Email",
    CONVERT(VARCHAR(10), a.ultcomps, 23) AS "Ultima Compra",
    CONVERT(VARCHAR(10), a.dataincs, 23) AS "Data Inclusao",
    RTRIM( a.inativas) AS "Inativo"
FROM sljcli a WITH(NOLOCK)
    LEFT OUTER JOIN sljscli b WITH(NOLOCK) ON a.situas = b.codigos
    LEFT OUTER JOIN sljfpubl c WITH(NOLOCK) ON a.fpubls = c.cods
    LEFT OUTER JOIN sljcli d WITH(NOLOCK) ON a.contavens = d.iclis
WHERE a.grupos IN ('CLIENTES',
                   'ACOMP LOJA',
                   'P SOCIAL',
                   'PRODUTORES',
                   'IMPRENSA')
    AND LEN(a.iclis) > 7
AND a.ultcomps IS NOT NULL
    '''
    return query

# Query to dataframe
def sqlToDataframe(query):
    conn = conn_pymssql()
    read_sql = pd.read_sql(query, conn)
    df = pd.DataFrame(read_sql)
    df = df.astype(str)
    df.to_csv('outputs/clientes.csv', index=False, sep=';')
    
    return df

# Criar conexão com o SFTP

def push_file_to_server():

    # Usuario e senha para conexão com o SFTP
    host = "ftp.jvphotos.com.br"
    user = "etl@jvphotos.com.br"
    passw = 'etl@2022@'

    # connect to the FTP server
    ftp = FTP("ftp.jvphotos.com.br")
    ftp.login("etl@jvphotos.com.br", "etl@2022@")

    print("Connection succesfully stablished ... ")

    # force UTF-8 encoding
    ftp.encoding = "utf-8"

    localfile='outputs/clientes.csv'
    remotefile='clientes.csv'

    with open(localfile, "rb") as file:
        print("Uploading file to server ... ")
        ftp.storbinary('STOR %s' % remotefile, file)
        print("File uploaded successfully ... ")

        ftp.quit()