import re;
arquivo = open('teste03', 'r')
#Cada linha do arquivo fica em um índice diferente
arquivolist = list(arquivo)     
REDO = []                    

#Cria um padrão para procurar uma T[x], x é qualquer valor entre  0  à 9
checkvalue = re.compile(r'T[0-9]*,', re.IGNORECASE)
#Procura a palavra "commit"
commit = re.compile(r'commit', re.IGNORECASE)
#Ignora as palavras descritas e coloca as demais em uma lista com .findall
extracT = re.compile(r'(?!commit\b)(?!CKPT\b)(?!Start\b)\b\w+', re.IGNORECASE)
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
print("", variaveis)
end = 0

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
arquivo.close()
