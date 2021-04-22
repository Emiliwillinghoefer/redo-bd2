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

    cursor, connection = conexao()
    columns = variaveis.keys()
    values = variaveis.values()
    create_query_columns = []
    create_statics = "DROP TABLE IF EXISTS teste; CREATE TABLE teste(id int primary key);"
    cursor.execute(create_statics)
    for column in columns:
        create_query_columns.append(column + " int")
    create_query_columns = ", " . join(create_query_columns)

    create_query = "DROP TABLE IF EXISTS teste; CREATE TABLE teste(id int primary key not null, " + create_query_columns + " ); "
    cursor.execute(create_query)
    connection.commit()
    cursor.execute("SELECT * FROM TESTE;")
    cursor.close()
    return print("\n", create_query)

def inserting_table(variaveis):
    cursor, connection = conexao()
    columns = variaveis.keys()  
    values = variaveis.values()
    str_values = []
    for value in values:
        str_values.append(str(value))

    insert_query = "INSERT INTO teste (id, " + ", ".join(columns) + ") "
    insert_query += "VALUES (1, "+ ", ".join(str_values) + ")"
    cursor.execute(insert_query)
    connection.commit()
    cursor.close()
    return print("\n", insert_query)

def update_table(variaveis):
    cursor = conexao()
    id = 1
    columns = variaveis.keys()

    new_values = []
    for column in columns:
        new_values.append(column + "=" + str(variaveis[column]))

    print(new_values)
    new_values = ", ".join(new_values)
    update_query = "UPDATE teste SET " + new_values + " WHERE id = " + str(id)
    cursor.execute(insert_query)
    cursor.commit()
    cursor.close()
    return print(update_query)

def log():
    
    arquivo = open('teste03', 'r')

    arquivolist = list(arquivo)     
    REDO = []                    


    checkvalue = re.compile(r'T[0-9]*,', re.IGNORECASE)
    commit = re.compile(r'commit', re.IGNORECASE)
    startCheckpoint = re.compile(r'Start\sCKPT', re.IGNORECASE)
    endCheckpoint = re.compile(r'End\sCKPT', re.IGNORECASE)
    extracT = re.compile(r'(?!commit\b)(?!Start\b)\b\w+', re.IGNORECASE)
    words = re.compile(r'\w+', re.IGNORECASE)

    valores = words.findall(arquivolist[0])
    variaveis = {}

    
    for i in range(0,len(valores),2): 
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
    
    #Procura todos os commits antes do Start CKPT e guarda a transação
    for i in range(0, indexStart):
        if commit.search(arquivolist[i]):
            commitAntes.append(extracT.findall(arquivolist[i])[0])
    
    #Procura todos os commits entre o Start e o End e guarda a transação
    for i in range(indexStart, indexEnd):
        if commit.search(arquivolist[i]):
            commitEntre.append(extracT.findall(arquivolist[i])[0])

    #Procura todos os commits depois do End CKPT e guarda a transação
    for i in range(indexEnd, len(arquivolist)):
        if commit.search(arquivolist[i]):
            commitDepois.append(extracT.findall(arquivolist[i])[0])
    
    print("Commit Antes: ", commitAntes)
    print("Commit Entre: ", commitEntre) #Será o nosso REDO
    print("Commit Depois: ", commitDepois)

    print(variaveis)
    for j in range(1, len(arquivolist)):
        linha = arquivolist[j]
        if (checkvalue.search(linha)):
            match = (extracT.findall(linha))
            if(match[0] in commitEntre and variaveis[match[2]] != match[3]):
                print(match)
                variaveis[match[2]] = match[3]
   
    print("Resultado:", variaveis)

    update_table(variaveis)
    arquivo.close()
    
log()