# Semantix

## Qual o objetivo do comando cache em Spark?
R> Objetivo é tolerar falhas se quaisquer partição de um RDD for perdido, ele vai automaticamenteele ser recalculado usando as transformações que originalmente o criaram de inicio.


## O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
R>Enquanto o Spark faz todo trabalho na memória, o MapReduce perde um pouco nisso, pois ele grava e lê de um disco rigido.


## Qual é a função do SparkContext ?
R>Ele é a principal funcão, nele configuramos opçoes de uso, no qual seria criação de RDD, recursos de memorias e processadores.

## Explique com suas palavras o que é Resilient Distributed Datasets (RDD)
R> Um RDD é, essencialmente, a representação do Spark de um conjunto de dados, distribuído em várias máquinas(Distribuited) e tolerante a falhas(Resilient). Um RDD pode vir de qualquer fonte de dados, por ex. arquivos de texto, um banco de dados via JDBC, etc.


## GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
R> Os 2 produzirão a mesma resposta, reduceByKey funciona muito melhor em um grande conjunto de dados porque o Spark sabe que pode combinar a saída com uma chave comum em cada partição antes de misturar os dados.

Por outro lado, groupByKey, todos os pares de valores-chave são misturados. Isso é; um monte de dados desnecessários para serem transferidos pela rede.



## Explique o que o código Scala abaixo 
```scala
val textFile = sc.textFile("hdfs://..." )
val counts = textFile.flatMap( line => line . split ( " " ))
           .map(word => ( word , 1 ))
           .reduceByKey( _ + _ )
.counts.saveAsTextFile("hdfs://..." )
```
1: Leitura de arquivo.<br /><br />
2: Feito split na linha(Quebrou a linha com o espaço), cada palavra se torna uma coleção de palavras<br />
3: Feito um mapeamento de key-value, com a chave igual a palavra e valor 1<br />
4: Valores são salvos por chave com o operador soma<br />
5: Feito uma contagem de palavras e salvo em um arquivo txt <br />
