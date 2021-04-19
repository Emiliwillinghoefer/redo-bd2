import psycopg2
import re

def conexao():
    connection = psycopg2.connect(user="postgres",
                                password="admin",
                                host="127.0.0.1",
                                port="5432",
                                database="redobd")

    print('sucesso')

def create_table(colunas):
    #cursor = connection.cursor()
    chaves = colunas.keys()
    print("chave:", chaves)
    chaves = ",".join([chave + " int" for chave in chaves])
    print("chave:", chaves)
    return
    cursor.execute("DROP TABLE IF EXISTS teste; CREATE TABLE teste(a int, b int, c int, d int, e int,f int); ")
    cursor.execute("insert INTO teste (id, a, b, c, d, e, f) VALUES (%s, %s, %s, %s, %s, %s, %s);", (idi, a, b, c, d, e, f))

    connection.commit()
    cursor.execute("SELECT * from teste")
    record = cursor.fetchall()
    print("Result ", record)





def valida_log():

    arquivo = open('teste02-2 (1)', 'r')
    arquivolist = list(arquivo)     #cria uma lista com o .txt
    REDO = []                       #salva quem vai ser feito REDO

    #Variaveis p/ identificar se existe no .txt
    checkvalue = re.compile(r'T[0-9]*,', re.IGNORECASE) #re.IGNORECASE -> ignorar maiuscula ou minuscula
    commit = re.compile(r'commit', re.IGNORECASE) #re.IGNORECASE -> ignorar se maiuscula ou minuscula
    extracT = re.compile(r'(?!commit\b)(?!CKPT\b)(?!Start\b)\b\w+', re.IGNORECASE) #Ignora as palavras descritas e coloca as demais em uma lista com .findall
    words = re.compile(r'\w+', re.IGNORECASE)   #Utilizado p/ pegar o valor das variaveis

    valores = words.findall(arquivolist[0])
    variaveis = {}
    for i in range(0,len(valores),2): #Iniciar primeiros valores das variáveis (A B C...)
        variaveis[valores[i]]= valores[i+1]
    del valores
    print("", variaveis)
    end = 0
    create_table(variaveis)
    for linha in reversed(arquivolist): #Verificar os casos e criar as listas de REDO
        if commit.search(linha):  #Procura commit
            REDO.append(extracT.findall(linha)[0])
        


    print("Aplicado REDO:", REDO, "\n")

    for j in range(1,len(arquivolist)-1,1):
        linha = arquivolist[j]    
        if (checkvalue.search(linha)):
            if(extracT.findall(linha)[0] in REDO):           
                variaveis[words.findall(linha)[1]] = words.findall(linha)[2]
    

    print("Resultado:", variaveis)
    arquivo.close()

    return variaveis


#Resultado: {'A': '25', 'B': '30', 'C': '90', 'D': '40', 'E': '28', 'F': '1', 'CKPT': 'T3'}
'''
def verifica_bd():
    cursor.execute("SELECT * from teste WHERE a=a, b=b,")


def insere_valor(variaveis):
    for que anda de 2 em 2:
        a = valor[i] 
'''

idi = 1
a = 20
b = 20
c = 70
d = 50
e = 17
f = 1

#conexao()
valida_log()

