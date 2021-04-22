import psycopg2
import re

def conexao():
    connection = psycopg2.connect(user="postgres",
                                  password="admin",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="bancodedados")

    cursor = connection.cursor()
    return cursor, connection

def create_table(variaveis):

    #cursor, connection = conexao()
    columns = variaveis.keys()
    values = variaveis.values()
    create_query_columns = []
    #create_statics = "DROP TABLE IF EXISTS teste; CREATE TABLE teste(id int primary key);"
    #cursor.execute(create_statics)
    #print(create_statics)
    for column in columns:
        create_query_columns.append(column + " int")
    create_query_columns = ", " . join(create_query_columns)

    '''for i in columns:
        create_query = "ALTER TABLE teste ADD COLUMN, "+ list(columns.keys()) + ";"
        cursor.execute(create_query)
        cursor.execute("SELECT * FROM teste;")'''

    create_query = "DROP TABLE IF EXISTS teste; CREATE TABLE teste(id int primary key not null, " + create_query_columns + " ); "
    #cursor.execute(create_query)
    #connection.commit()
    #cursor.execute("SELECT * FROM TESTE;")
    #cursor.close()
    return print("\n", create_query)


def inserting_table(variaveis):
    #cursor, connection = conexao()
    columns = variaveis.keys()  
    values = variaveis.values()
    str_values = []
    for value in values:
        str_values.append(str(value))
    # str_values = [str(value) for value in values]

    insert_query = "INSERT INTO teste (id, " + ", ".join(columns) + ") "
    insert_query += "VALUES (1, "+ ", ".join(str_values) + ")"
    #cursor.execute(insert_query)
    #connection.commit()
    #cursor.close()
    return print("\n", insert_query)

def update_table(variaveis):
    #cursor = conexao()
    id = 1
    columns = variaveis.keys()

    new_values = []
    for column in columns:
        new_values.append(column + "=" + str(variaveis[column]))

    print(new_values)
    new_values = ", ".join(new_values)
    update_query = "UPDATE teste SET " + new_values + " WHERE id = " + str(id)
    #cursor.execute(insert_query)
    #cursor.commit()
    #cursor.close()
    return print(update_query)

def log():
    
    arquivo = open('teste_final', 'r')
    #Cada linha do arquivo fica em um índice diferente
    arquivolist = list(arquivo)     
    REDO = []                    

    #Cria um padrão para procurar uma T[x], x é qualquer valor entre  0  à 9
    checkvalue = re.compile(r'T[0-9]*,', re.IGNORECASE)
    #Procura a palavra "commit"
    commit = re.compile(r'commit', re.IGNORECASE)
    checkpoint = re.compile(r'CKPT', re.IGNORECASE)
    startCheckpoint = re.compile(r'Start\sCKPT', re.IGNORECASE)
    endCheckpoint = re.compile(r'End\sCKPT', re.IGNORECASE)
    #Ignora as palavras descritas e coloca as demais em uma lista com .findall
    extracT = re.compile(r'(?!commit\b)(?!CKPT\b)(?!Start\b)\b\w+', re.IGNORECASE)
    #Utilizado p/ pegar o valor das variaveis
    words = re.compile(r'\w+', re.IGNORECASE)


    #aqui colocar todos os valores iniciais em um vetor um em cada indice['A', '20', 'B', '20'] 
    valores = words.findall(arquivolist[0])
    variaveis = {}

    
    #i assume valores de 0 à 10, de 2 em 2
    for i in range(0,len(valores),2): #Iniciar primeiros valores das variáveis (A B C...)
        #Valores[i]= indices do dicionário que serão as letras de A à F
        #Valores[i+1] = Valores de cada "Letra"
        variaveis[valores[i]]= valores[i+1]

    print("Valores Iniciais\n", variaveis)
    
    create_table(variaveis)
    inserting_table(variaveis)

    indexEnd = 0
    indexStart = 0
    commitAntes = []
    commitEntre = []
    commitDepois = []

    #Procura o Start CKPT e o End CKPT e guarda os índices
    for i in range(0, len(arquivolist), 1):
        if startCheckpoint.search(arquivolist[i]):
            indexStart = i
        if endCheckpoint.search(arquivolist[i]):
            indexEnd = i
    
    #Procura todos os commits antes do Start CKPT
    for i in range(0, indexStart):
        if commit.search(arquivolist[i]):
            commitAntes.append(extracT.findall(arquivolist[i])[0])
    
    #Procura todos os commits entre o Start e o End
    for i in range(indexStart, indexEnd):
        if commit.search(arquivolist[i]):
            commitEntre.append(extracT.findall(arquivolist[i])[0])

    #Procura todos os commits depois do End CKPT
    for i in range(indexEnd, len(arquivolist)):
        if commit.search(arquivolist[i]):
            commitDepois.append(extracT.findall(arquivolist[i])[0])
    
    print("Commit Antes: ", commitAntes)
    print("Commit Entre: ", commitEntre)
    print("Commit Depois: ", commitDepois)

    '''
    for linha in reversed(arquivolist): #Verificar os casos e criar as listas de REDO
            if commit.search(linha):  #Procura commit
                #Guarda em REDO todas as transações que realizaram commit
                REDO.append(extracT.findall(linha)[0])
    print("\nAplicado REDO:", REDO, "\n")'''

    #Aqui está imprimindo certo todas as colunas e os valores
    print("Var = ", variaveis)

    for j in range(1, len(arquivolist)):
        #Pega a linha nesse formato <T3,1,A,25>
        linha = arquivolist[j]
        #Pega todas as transações de cada linha
        if (checkvalue.search(linha)):
            #Pega as transações em redo
            if(extracT.findall(linha)[0] in commitEntre):
                #variaveis na posição 1, recebe a letra
                variaveis[words.findall(linha)[1]] = words.findall(linha)[2]

    print("Resultado:", variaveis)
    update_table(variaveis)

    arquivo.close()
    
log()