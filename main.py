import psycopg2
import re

def conexao():
    connection = psycopg2.connect(user="postgres",
                                  password="admin",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="bancodedados")

    cursor = connection.cursor()
    return cursor

def create_table(variaveis):

    cursor = conexao()
    columns = variaveis.keys()
    values = variaveis.values()
    create_query_columns = []
    for column in columns:
        create_query_columns.append(column + " int")
    
    create_query_columns = ", ".join(create_query_columns)
    
    create_query = "CREATE TABLE test (id integer primary key, " + create_query_columns + ")"
    cursor.execute(create_query)
    cursor.close()
    return print("\n", create_query)


def inserting_table(variaveis):
    cursor = conexao()
    columns = variaveis.keys()
    values = variaveis.values()
    str_values = []
    for value in values:
        str_values.append(str(value))
    # str_values = [str(value) for value in values]

    insert_query = "INSERT INTO test (id, " + ", ".join(columns) + ") "
    insert_query += "VALUES (1, "+ ", ".join(str_values) + ")"
    cursor.execute(insert_query)
    cursor.commit()
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
    update_query = "UPDATE test SET " + new_values + " WHERE id = " + str(id)
    cursor.execute(insert_query)
    cursor.commit()
    cursor.close()
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
    #Ignora as palavras descritas e coloca as demais em uma lista com .findall
    extracT = re.compile(r'(?!commit\b)(?!CKPT\b)(?!Start\b)\b\w+', re.IGNORECASE)
    checkpoint = re.compile(r'End', re.IGNORECASE)
    #Utilizado p/ pegar o valor das variaveis
    words = re.compile(r'\w+', re.IGNORECASE)
    pegaId = re.compile(r',[0-9]*,')


    #aqui colocar todos os valores iniciais em um vetor um em cada indice['A', '20', 'B', '20'] 
    valores = words.findall(arquivolist[0])
    #declara um dicionario
    variaveis = {}

    
    #i assume valores de 0 à 10, de 2 em 2
    for i in range(0,len(valores),2): #Iniciar primeiros valores das variáveis (A B C...)
        #Valores[i]= indices do dicionário que serão as letras de A à F
        #Valores[i+1] = Valores de cada "Letra"
        variaveis[valores[i]]= valores[i+1]

    del valores
    print("Valores Iniciais\n", variaveis)
    
    create_table(variaveis)
    inserting_table(variaveis)

    end = 0
    indice = 0
    for linha in reversed(arquivolist): #Verificar os casos e criar as listas de REDO
        if commit.search(linha):  #Procura commit
            #Guarda em REDO todas as transações que realizaram commit
            REDO.append(extracT.findall(linha)[0])
        
         
            
    print("\nAplicado REDO:", REDO, "\n")

    getId = []
    #For que nao pega nem o primeiro, nem o último indice.
    for j in range(1,len(arquivolist)-1,1):
        #Pega a linha nesse formato <T3,1,A,25>
        linha = arquivolist[j]
        #Pega só as transações de cada linha(se existir)
        if (checkvalue.search(linha)):
            if(extracT.findall(linha)[0] in REDO):
                variaveis[words.findall(linha)[1]] = words.findall(linha)[2]
                

    print("Resultado:", variaveis)
    update_table(variaveis)

    arquivo.close()

log()