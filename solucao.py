import sqlite3
import csv
from fastapi import FastAPI, Depends

app = FastAPI()

# Função para leitura de arquivos CSV
def ler_csv(nome_arquivo):
    with open(nome_arquivo, 'r') as f:
        leitor = csv.DictReader(f)
        dados = list(leitor)
    return dados

# Carregando os dados dos CSVs
funcionarios = ler_csv('funcionarios.csv')
cargos = ler_csv('cargos.csv')
departamentos = ler_csv('departamentos.csv')
historico_salarios = ler_csv('historico_salarios.csv')
dependentes = ler_csv('dependentes.csv')
projetos_desenvolvidos = ler_csv('projetos_desenvolvidos.csv')
recursos_projeto = ler_csv('recursos_projeto.csv')

# Função para gerenciar a conexão com o banco de dados
def get_db():
    conn = sqlite3.connect('empresa.db')
    try:
        yield conn
    finally:
        conn.close()

# Inicializando o banco de dados e populando tabelas
def inicializar_banco():
    conn = sqlite3.connect('empresa.db')
    cursor = conn.cursor()

    # Resetando tabelas existentes
    tabelas = [
        "funcionarios", "cargos", "departamentos",
        "historico_salarios", "dependentes",
        "projetos_desenvolvidos", "recursos_projeto"
    ]
    for tabela in tabelas:
        cursor.execute(f"DROP TABLE IF EXISTS {tabela}")

    # Criando tabelas
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS funcionarios (
        id_funcionario INT PRIMARY KEY,
        nome TEXT NOT NULL,
        idade INT NOT NULL,
        data_admissao TEXT NOT NULL,
        id_cargo INT NOT NULL,
        id_departamento INT NOT NULL,
        FOREIGN KEY (id_cargo) REFERENCES cargos(id_cargo),
        FOREIGN KEY (id_departamento) REFERENCES departamentos(id_departamento)
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cargos (
        id_cargo INT PRIMARY KEY,
        titulo TEXT NOT NULL,
        nivel TEXT NOT NULL,
        salario_base REAL NOT NULL
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS departamentos (
        id_departamento INT PRIMARY KEY,
        nome_departamento TEXT NOT NULL,
        localizacao TEXT NOT NULL
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS historico_salarios (
        id_funcionario INT,
        mes_ano TEXT NOT NULL,
        salario_recebido REAL NOT NULL,
        PRIMARY KEY (id_funcionario, mes_ano),
        FOREIGN KEY (id_funcionario) REFERENCES funcionarios(id_funcionario)
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dependentes (
        id_funcionario INT,
        nome_dependente TEXT NOT NULL,
        data_nascimento TEXT NOT NULL,
        parentesco TEXT NOT NULL,
        PRIMARY KEY (id_funcionario, nome_dependente),
        FOREIGN KEY (id_funcionario) REFERENCES funcionarios(id_funcionario)
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS projetos_desenvolvidos (
        id_projeto INT PRIMARY KEY,
        nome_projeto TEXT NOT NULL,
        descricao TEXT NOT NULL,
        data_inicio TEXT NOT NULL,
        data_conclusao TEXT,
        id_funcionario INT NOT NULL,
        custo_projeto REAL NOT NULL,
        status TEXT NOT NULL,
        categoria TEXT NOT NULL,
        FOREIGN KEY (id_funcionario) REFERENCES funcionarios(id_funcionario)
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS recursos_projeto (
        id_recurso INT PRIMARY KEY,
        id_projeto INT NOT NULL,
        descricao_recurso TEXT NOT NULL,
        tipo_recurso TEXT NOT NULL,
        quantidade INT NOT NULL,
        data_utilizacao TEXT NOT NULL,
        custo_unitario REAL NOT NULL,
        custo_total REAL NOT NULL,
        FOREIGN KEY (id_projeto) REFERENCES projetos_desenvolvidos(id_projeto)
    )''')

    # Função para inserir dados no banco
    def inserir_dados(tabela, dados):
        colunas = ', '.join(dados[0].keys())
        placeholders = ', '.join('?' * len(dados[0]))
        sql = f'INSERT INTO {tabela} ({colunas}) VALUES ({placeholders})'
        cursor.executemany(sql, [tuple(row.values()) for row in dados])

    # Inserindo dados dos CSVs
    inserir_dados('funcionarios', funcionarios)
    inserir_dados('cargos', cargos)
    inserir_dados('departamentos', departamentos)
    inserir_dados('historico_salarios', historico_salarios)
    inserir_dados('dependentes', dependentes)
    inserir_dados('projetos_desenvolvidos', projetos_desenvolvidos)
    inserir_dados('recursos_projeto', recursos_projeto)

    conn.commit()
    conn.close()

# Inicializar o banco ao rodar o script
inicializar_banco()

# Endpoints FastAPI
@app.get("/")
def read_root():
    return {"message": "Servidor FastAPI está funcionando!"}

@app.get("/consulta2")
def consulta2(db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute('''
    SELECT descricao_recurso, SUM(quantidade) AS quantidade_total
    FROM recursos_projeto
    GROUP BY descricao_recurso
    ORDER BY quantidade_total DESC
    LIMIT 3
    ''')
    resultados = cursor.fetchall()
    return [{"recurso": r[0], "quantidade_total": r[1]} for r in resultados]

@app.get("/consulta3")
def consulta3(db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute('''
    SELECT departamentos.nome_departamento, SUM(projetos_desenvolvidos.custo_projeto) AS custo_total
    FROM projetos_desenvolvidos
    JOIN funcionarios ON projetos_desenvolvidos.id_funcionario = funcionarios.id_funcionario
    JOIN departamentos ON funcionarios.id_departamento = departamentos.id_departamento
    WHERE projetos_desenvolvidos.status = 'Concluído'
    GROUP BY departamentos.nome_departamento
    ''')
    resultados = cursor.fetchall()
    return [{"departamento": r[0], "custo_total": r[1]} for r in resultados]

@app.get("/consulta5")
def consulta5(db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute('''
    SELECT projetos_desenvolvidos.nome_projeto, COUNT(dependentes.id_funcionario) AS num_dependentes
    FROM projetos_desenvolvidos
    JOIN dependentes ON dependentes.id_funcionario = projetos_desenvolvidos.id_funcionario
    GROUP BY projetos_desenvolvidos.id_projeto
    ORDER BY num_dependentes DESC
    LIMIT 1
    ''')
    resultados = cursor.fetchall()
    return [{"projeto": r[0], "num_dependentes": r[1]} for r in resultados]

# Executar o servidor
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
