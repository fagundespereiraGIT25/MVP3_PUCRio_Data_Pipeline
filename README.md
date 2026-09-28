# MVP de Engenharia de Dados

## Pipeline de Dados para Análise de Sentimento e Crítica de Cinema --- IMDb e Rotten Tomatoes

**Aluno:** Fagundes Pereira da Silva\
**Curso:** Especialização em Ciência de Dados e Analytics --- PUC-Rio\
**MVP:** Construção de um Pipeline de Dados na Nuvem\
**Plataforma:** Databricks Free Edition --- Serverless\
**Arquitetura:** Dados Brutos → Bronze → Prata → Ouro\
**Tecnologias:** Databricks, Apache Spark/PySpark, Delta Lake, Unity
Catalog, Python e Kaggle

------------------------------------------------------------------------

## 1. Contexto de Negócios e Perguntas (Etapas 2 e 4.1)

### 1.1 Motivação pessoal e contexto do problema

A escolha do tema deste MVP parte de dois interesses que se encontram
neste trabalho: o aprimoramento técnico em **Engenharia de Dados** e meu
interesse pessoal por **cinema e crítica de cinema**.

Mais do que utilizar avaliações de filmes apenas como um conjunto de
dados conveniente para exercitar técnicas de ingestão e transformação,
procurei construir um pipeline que permitisse compreender como
**sentimentos, opiniões e ponderações sobre filmes** aparecem em fontes
distintas e como essas impressões podem ser capturadas, limpas,
organizadas e integradas sem que o processo de engenharia elimine sua
semântica original.

Esse aspecto foi importante na concepção do trabalho. Uma avaliação
classificada como `positive` no IMDb e uma crítica classificada como
`Fresh` no Rotten Tomatoes podem ser harmonizadas para permitir análises
conjuntas, mas não são, necessariamente, manifestações produzidas pelo
mesmo processo ou pela mesma população. Assim, o objetivo não foi
simplesmente converter valores diferentes em um rótulo comum, mas
preservar a origem, a classificação original e o contexto de cada
avaliação.

O MVP também foi concebido para exercitar o raciocínio de ponta a ponta
de Engenharia de Dados: partir de um problema, identificar fontes
adequadas, coletar e armazenar os dados em nuvem, tratar problemas de
qualidade, modelar as informações e disponibilizá-las para responder
perguntas analíticas.

### 1.2 Um desafio conhecido desde a escolha das fontes

Desde a seleção dos datasets era conhecida a diferença significativa de
volume entre as duas fontes.

O IMDb utilizado neste projeto possui **50.000 avaliações na origem**,
enquanto o arquivo de avaliações de críticos do Rotten Tomatoes possui
**1.130.017 registros na origem**.

Essa assimetria foi aceita **a priori**, pois o propósito do MVP não era
produzir duas amostras artificialmente balanceadas, mas trabalhar com
fontes públicas reais e preservar suas características.

Durante o desenvolvimento, entretanto, essa diferença tornou-se também
um dos desafios técnicos e analíticos do MVP. Ela exigiu atenção para
que:

-   a integração não eliminasse a identificação da fonte;
-   os resultados agregados não fossem interpretados como se IMDb e
    Rotten Tomatoes tivessem o mesmo peso;
-   a harmonização de sentimentos não fosse confundida com equivalência
    metodológica entre as fontes;
-   a granularidade dos registros fosse preservada;
-   as análises por fonte permanecessem disponíveis mesmo após a
    integração;
-   as conclusões considerassem explicitamente a diferença de
    representatividade dos datasets.

Assim, a diferença de volume deixou de ser apenas uma característica das
bases e passou a fazer parte do próprio problema de Engenharia de Dados.

### 1.3 Problema

O problema tratado pelo MVP pode ser resumido da seguinte forma:

> **Como construir, em ambiente de nuvem, um pipeline de dados capaz de
> integrar avaliações de filmes provenientes de fontes heterogêneas,
> preservando sua rastreabilidade e semântica, tratando problemas de
> qualidade e disponibilizando dados confiáveis para analisar
> sentimentos e padrões de crítica cinematográfica?**

### 1.4 Objetivo geral

Construir um pipeline funcional de Engenharia de Dados em nuvem que
percorra o ciclo completo entre a coleta dos dados brutos e sua
disponibilização para análise, utilizando avaliações de filmes do IMDb e
do Rotten Tomatoes.

### 1.5 Objetivos específicos

O MVP busca:

-   coletar os datasets públicos e registrar claramente suas fontes;
-   preservar os arquivos originais em uma área de Dados Brutos;
-   estruturar a arquitetura Medallion nas camadas Bronze, Prata e Ouro;
-   manter rastreabilidade entre os registros tratados e suas fontes;
-   avaliar completude, consistência, unicidade, plausibilidade e
    valores extremos;
-   remover duplicidades sem alterar indevidamente o conteúdo das
    avaliações;
-   harmonizar as classificações de sentimento preservando os valores
    originais;
-   modelar uma camada Ouro adequada ao consumo analítico;
-   validar granularidade e integridade referencial;
-   analisar como sentimentos e padrões de crítica se comportam nas
    fontes utilizadas.

### 1.6 Perguntas de negócio

As perguntas formuladas para orientar o pipeline e a análise foram:

1.  Qual é a distribuição de avaliações positivas e negativas em cada
    fonte?
2.  Existem diferenças entre as proporções de sentimento observadas no
    IMDb e no Rotten Tomatoes?
3.  No Rotten Tomatoes, existe diferença na proporção de avaliações
    positivas entre **Top Critics** e os demais críticos?
4.  Como o sentimento das avaliações do Rotten Tomatoes se distribui ao
    longo do tempo?
5.  Quais filmes concentram maior quantidade de avaliações no Rotten
    Tomatoes?
6.  Entre filmes com volume relevante de avaliações, quais apresentam as
    maiores proporções de avaliações positivas?
7.  Como se distribui o volume de avaliações por filme e que critério
    pode ser utilizado para evitar comparações baseadas em amostras
    muito pequenas?

------------------------------------------------------------------------

## 2. Busca pelos Dados e Fontes (Etapa 4.1)

Foram selecionados dois datasets públicos disponíveis no Kaggle, ambos
relacionados a avaliações cinematográficas, porém com estruturas e
granularidades diferentes.

### 2.1 Fonte 1 --- IMDb Dataset of 50K Movie Reviews --- 50 mil avaliações textuais para classificação binária de sentimento

**Nome no Kaggle:** *IMDb Dataset of 50K Movie Reviews*\
**Identificador utilizado na ingestão:**
`lakshmi25npathi/imdb-dataset-of-50k-movie-reviews`\
**Arquivo utilizado:** `IMDB Dataset.csv`\
**Volume original utilizado no MVP:** 50.000 avaliações\
**Estrutura principal:** uma avaliação textual por linha, acompanhada do
sentimento original.

Campos de origem:

  -----------------------------------------------------------------------
  Campo                   Tipo de origem          Significado
  ----------------------- ----------------------- -----------------------
  `review`                texto                   Conteúdo textual da
                                                  avaliação

  `sentiment`             categórico              Classificação original
                                                  `positive` ou
                                                  `negative`
  -----------------------------------------------------------------------

**URL da fonte:**\
https://www.kaggle.com/datasets/lakshmi25npathi/imdb-dataset-of-50k-movie-reviews

A própria página do Kaggle descreve o dataset como destinado à
classificação binária de sentimento e informa que a licença está
classificada como **"Other (specified in description)"**. Por esse
motivo, neste MVP os dados são utilizados exclusivamente para finalidade
acadêmica e não são redistribuídos no repositório.

### 2.2 Fonte 2 --- Rotten Tomatoes Movies and Critic Reviews Dataset --- filmes e avaliações de críticos profissionais

**Nome no Kaggle:** *Rotten Tomatoes movies and critic reviews dataset*\
**Identificador utilizado na ingestão:**
`stefanoleone992/rotten-tomatoes-movies-and-critic-reviews-dataset`\
**Arquivos utilizados:**

-   `rotten_tomatoes_movies.csv`
-   `rotten_tomatoes_critic_reviews.csv`

**Volume original de avaliações utilizado no MVP:** 1.130.017 registros
de críticas.

**URL da fonte:**\
https://www.kaggle.com/datasets/stefanoleone992/rotten-tomatoes-movies-and-critic-reviews-dataset

O dataset reúne informações de filmes e críticas profissionais do Rotten
Tomatoes. Referências públicas que reutilizam o conjunto original
identificam-no como disponibilizado em **CC0/Public Domain**. Para a
reprodução do trabalho, a página original do Kaggle deve ser considerada
a referência principal para os termos vigentes.

Principais campos utilizados das avaliações:

  Campo                    Significado
  ------------------------ -------------------------------------
  `rotten_tomatoes_link`   Chave de relacionamento com o filme
  `critic_name`            Nome do crítico
  `top_critic`             Indicador de Top Critic
  `publisher_name`         Publicação
  `review_type`            Classificação original da crítica
  `review_score`           Nota original, quando disponível
  `review_date`            Data da avaliação
  `review_content`         Conteúdo textual da crítica

O arquivo de filmes fornece, entre outros atributos, o identificador
`rotten_tomatoes_link`, o título `movie_title`, datas, duração e
métricas agregadas do Tomatometer e da audiência.

### 2.3 Rastreabilidade

Os dados brutos não são redistribuídos pelo GitHub. A rastreabilidade é
mantida pela documentação das URLs, identificadores dos datasets, nomes
dos arquivos e metadados técnicos adicionados durante a ingestão.

------------------------------------------------------------------------

## 3. Carga dos Dados (Etapa 4.2)

A solução foi executada no **Databricks Free Edition**, utilizando o
ambiente Serverless.

A coleta foi automatizada por notebook com a biblioteca `kagglehub`. Os
datasets foram baixados para o cache temporário da biblioteca e, em
seguida, os arquivos esperados foram copiados para um **Volume do Unity
Catalog**, que funciona como área de Dados Brutos/Landing Zone.

``` text
Kaggle
   │
   ▼
kagglehub.dataset_download()
   │
   ▼
cache temporário
   │
   ▼
/Volumes/mvp3_eng_dados/dados_brutos/volume_dados_brutos
   │
   ▼
Camada Bronze
```

Arquivos armazenados:

``` text
IMDB Dataset.csv
rotten_tomatoes_movies.csv
rotten_tomatoes_critic_reviews.csv
```

O processo está implementado no notebook:

``` text
01_ingestao_dados_brutos
```

A área de Dados Brutos não é tratada como uma quarta camada da
arquitetura Medallion; sua função é preservar os arquivos originais
antes da estruturação em tabelas.

``` markdown
```

------------------------------------------------------------------------

## 4. Modelagem e Catálogo de Dados (Etapa 4.3)

### 4.1 Arquitetura implementada

A implementação utiliza o catálogo Databricks `mvp3_eng_dados`. A área
`dados_brutos` contém o Volume da Landing Zone; as tabelas persistidas
estão organizadas nos schemas **Bronze, Prata e Ouro**.

Os metadados foram extraídos diretamente de
`system.information_schema.columns`. A exportação contém **134 registros
de colunas**, distribuídos por **11 tabelas persistidas**: 3 na Bronze,
4 na Prata e 4 na Ouro.

``` text
mvp3_eng_dados
├── dados_brutos
│   └── volume_dados_brutos
├── bronze
│   ├── imdb_avaliacoes
│   ├── rt_avaliacoes
│   └── rt_filmes
├── prata
│   ├── avaliacoes_harmonizadas
│   ├── imdb_avaliacoes
│   ├── rt_avaliacoes
│   └── rt_filmes
└── ouro
    ├── dim_data
    ├── dim_filme
    ├── dim_fonte
    └── fato_avaliacao
```

A Landing Zone armazena arquivos e não aparece em
`information_schema.columns` como tabela.

### 4.2 Catálogo de dados transcrito

Os nomes das tabelas, nomes dos campos e tipos abaixo foram transcritos
dos **metadados reais exportados do Databricks**. A coluna `comment` do
`information_schema` está sem preenchimento na exportação; por isso, as
descrições e domínios documentais são registrados neste README com base
nas fontes e nas transformações efetivamente implementadas.

#### `bronze.imdb_avaliacoes`

Avaliações IMDb preservadas conforme a fonte, com metadados técnicos de
ingestão.

  ----------------------------------------------------------------------
  Campo               Tipo Databricks  Descrição        Domínio /
                                                        observação
  ------------------- ---------------- ---------------- ----------------
  `review`            `STRING`         Texto da         Conforme a fonte
                                       avaliação IMDb.  ou o tipo do
                                                        campo; sem
                                                        domínio
                                                        categórico
                                                        adicional
                                                        documentado.

  `sentiment`         `STRING`         Classificação    `positive` ou
                                       original de      `negative`
                                       sentimento do    (IMDb)
                                       IMDb.            

  `_fonte`            `STRING`         Metadado técnico Conforme a fonte
                                       com a fonte de   ou o tipo do
                                       origem.          campo; sem
                                                        domínio
                                                        categórico
                                                        adicional
                                                        documentado.

  `_arquivo_origem`   `STRING`         Metadado técnico Conforme a fonte
                                       com o arquivo de ou o tipo do
                                       origem.          campo; sem
                                                        domínio
                                                        categórico
                                                        adicional
                                                        documentado.

  `_data_ingestao`    `TIMESTAMP`      Data/hora        Conforme a fonte
                                       técnica da       ou o tipo do
                                       ingestão.        campo; sem
                                                        domínio
                                                        categórico
                                                        adicional
                                                        documentado.
  ----------------------------------------------------------------------

#### `bronze.rt_avaliacoes`

Avaliações de críticos do Rotten Tomatoes preservadas conforme a fonte,
com metadados técnicos.

  -----------------------------------------------------------------------------
  Campo                    Tipo Databricks Descrição            Domínio /
                                                                observação
  ------------------------ --------------- -------------------- ---------------
  `rotten_tomatoes_link`   `STRING`        Identificador/link   Conforme a
                                           do filme no Rotten   fonte ou o tipo
                                           Tomatoes.            do campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `critic_name`            `STRING`        Nome do crítico.     Conforme a
                                                                fonte ou o tipo
                                                                do campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `top_critic`             `STRING`        Indicador de Top     Conforme a
                                           Critic.              fonte ou o tipo
                                                                do campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `publisher_name`         `STRING`        Publicação associada Conforme a
                                           ao crítico.          fonte ou o tipo
                                                                do campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `review_type`            `STRING`        Classificação        `Fresh` ou
                                           original da crítica  `Rotten` (RT)
                                           Rotten Tomatoes.     

  `review_score`           `STRING`        Nota original da     Conforme a
                                           crítica, preservada  fonte ou o tipo
                                           no formato da fonte. do campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `review_date`            `STRING`        Data original da     Conforme a
                                           avaliação.           fonte ou o tipo
                                                                do campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `review_content`         `STRING`        Conteúdo textual da  Conforme a
                                           crítica.             fonte ou o tipo
                                                                do campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `_fonte`                 `STRING`        Metadado técnico com Conforme a
                                           a fonte de origem.   fonte ou o tipo
                                                                do campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `_arquivo_origem`        `STRING`        Metadado técnico com Conforme a
                                           o arquivo de origem. fonte ou o tipo
                                                                do campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `_data_ingestao`         `TIMESTAMP`     Data/hora técnica da Conforme a
                                           ingestão.            fonte ou o tipo
                                                                do campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.
  -----------------------------------------------------------------------------

#### `bronze.rt_filmes`

Dados de filmes do Rotten Tomatoes preservados conforme a fonte, com
metadados técnicos.

  --------------------------------------------------------------------------------------
  Campo                                Tipo          Descrição            Domínio /
                                       Databricks                         observação
  ------------------------------------ ------------- -------------------- --------------
  `rotten_tomatoes_link`               `STRING`      Identificador/link   Conforme a
                                                     do filme no Rotten   fonte ou o
                                                     Tomatoes.            tipo do campo;
                                                                          sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `movie_title`                        `STRING`      Título do filme.     Conforme a
                                                                          fonte ou o
                                                                          tipo do campo;
                                                                          sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `movie_info`                         `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `critics_consensus`                  `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `content_rating`                     `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `genres`                             `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `directors`                          `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `authors`                            `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `actors`                             `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `original_release_date`              `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `streaming_release_date`             `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `runtime`                            `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `production_company`                 `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `tomatometer_status`                 `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `tomatometer_rating`                 `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `tomatometer_count`                  `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `audience_status`                    `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `audience_rating`                    `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `audience_count`                     `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `tomatometer_top_critics_count`      `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `tomatometer_fresh_critics_count`    `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `tomatometer_rotten_critics_count`   `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `_fonte`                             `STRING`      Metadado técnico com Conforme a
                                                     a fonte de origem.   fonte ou o
                                                                          tipo do campo;
                                                                          sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `_arquivo_origem`                    `STRING`      Metadado técnico com Conforme a
                                                     o arquivo de origem. fonte ou o
                                                                          tipo do campo;
                                                                          sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `_data_ingestao`                     `TIMESTAMP`   Data/hora técnica da Conforme a
                                                     ingestão.            fonte ou o
                                                                          tipo do campo;
                                                                          sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.
  --------------------------------------------------------------------------------------

#### `ouro.dim_data`

Dimensão calendário derivada das datas disponíveis.

  ------------------------------------------------------------------------
  Campo              Tipo Databricks   Descrição         Domínio /
                                                         observação
  ------------------ ----------------- ----------------- -----------------
  `id_data`          `INT`             Chave da dimensão inteiro no padrão
                                       data.             AAAAMMDD

  `data_avaliacao`   `DATE`            Data da avaliação Conforme a fonte
                                       quando            ou o tipo do
                                       disponível.       campo; sem
                                                         domínio
                                                         categórico
                                                         adicional
                                                         documentado.

  `ano`              `INT`             Ano da data de    Conforme a fonte
                                       avaliação.        ou o tipo do
                                                         campo; sem
                                                         domínio
                                                         categórico
                                                         adicional
                                                         documentado.

  `trimestre`        `INT`             Trimestre da data Conforme a fonte
                                       de avaliação.     ou o tipo do
                                                         campo; sem
                                                         domínio
                                                         categórico
                                                         adicional
                                                         documentado.

  `mes`              `INT`             Mês da data de    Conforme a fonte
                                       avaliação.        ou o tipo do
                                                         campo; sem
                                                         domínio
                                                         categórico
                                                         adicional
                                                         documentado.

  `dia`              `INT`             Dia da data de    Conforme a fonte
                                       avaliação.        ou o tipo do
                                                         campo; sem
                                                         domínio
                                                         categórico
                                                         adicional
                                                         documentado.
  ------------------------------------------------------------------------

#### `ouro.dim_filme`

Dimensão de filmes do modelo analítico.

  -----------------------------------------------------------------------
  Campo             Tipo Databricks   Descrição         Domínio /
                                                        observação
  ----------------- ----------------- ----------------- -----------------
  `id_filme`        `STRING`          Identificador     Conforme a fonte
                                      técnico do filme. ou o tipo do
                                                        campo; sem
                                                        domínio
                                                        categórico
                                                        adicional
                                                        documentado.

  `titulo_filme`    `STRING`          Título do filme   Conforme a fonte
                                      harmonizado.      ou o tipo do
                                                        campo; sem
                                                        domínio
                                                        categórico
                                                        adicional
                                                        documentado.

  `link_filme`      `STRING`          Referência ao     Conforme a fonte
                                      filme na fonte,   ou o tipo do
                                      quando aplicável. campo; sem
                                                        domínio
                                                        categórico
                                                        adicional
                                                        documentado.
  -----------------------------------------------------------------------

#### `ouro.dim_fonte`

Dimensão de fonte das avaliações.

  -----------------------------------------------------------------------
  Campo             Tipo Databricks   Descrição         Domínio /
                                                        observação
  ----------------- ----------------- ----------------- -----------------
  `id_fonte`        `INT`             Identificador     Conforme a fonte
                                      técnico da fonte. ou o tipo do
                                                        campo; sem
                                                        domínio
                                                        categórico
                                                        adicional
                                                        documentado.

  `fonte`           `STRING`          Fonte da          Conforme a fonte
                                      avaliação.        ou o tipo do
                                                        campo; sem
                                                        domínio
                                                        categórico
                                                        adicional
                                                        documentado.
  -----------------------------------------------------------------------

#### `ouro.fato_avaliacao`

Tabela fato na granularidade de uma avaliação por registro.

  ------------------------------------------------------------------------------
  Campo                      Tipo           Descrição           Domínio /
                             Databricks                         observação
  -------------------------- -------------- ------------------- ----------------
  `id_avaliacao_global`      `STRING`       Identificador       Conforme a fonte
                                            técnico global e    ou o tipo do
                                            único da avaliação  campo; sem
                                            harmonizada.        domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `id_avaliacao`             `STRING`       Identificador       Conforme a fonte
                                            técnico da          ou o tipo do
                                            avaliação.          campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `id_fonte`                 `INT`          Identificador       Conforme a fonte
                                            técnico da fonte.   ou o tipo do
                                                                campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `id_filme`                 `STRING`       Identificador       Conforme a fonte
                                            técnico do filme.   ou o tipo do
                                                                campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `id_data`                  `INT`          Chave da dimensão   inteiro no
                                            data.               padrão AAAAMMDD

  `tipo_avaliador`           `STRING`       Tipo de avaliador,  Conforme a fonte
                                            quando aplicável.   ou o tipo do
                                                                campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `texto_avaliacao`          `STRING`       Texto da avaliação  Conforme a fonte
                                            no contrato         ou o tipo do
                                            harmonizado.        campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `classificacao_original`   `STRING`       Classificação       Conforme a fonte
                                            original preservada ou o tipo do
                                            da fonte.           campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `sentimento`               `STRING`       Sentimento          `POSITIVO` ou
                                            harmonizado.        `NEGATIVO`

  `nota_original`            `STRING`       Nota original       Conforme a fonte
                                            preservada sem      ou o tipo do
                                            normalização        campo; sem
                                            artificial.         domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `data_avaliacao`           `DATE`         Data da avaliação   Conforme a fonte
                                            quando disponível.  ou o tipo do
                                                                campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `nome_avaliador`           `STRING`       Nome do             Conforme a fonte
                                            avaliador/crítico   ou o tipo do
                                            quando disponível.  campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `publicacao`               `STRING`       Publicação          Conforme a fonte
                                            associada ao        ou o tipo do
                                            crítico quando      campo; sem
                                            disponível.         domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `eh_top_critic`            `BOOLEAN`      Indicador de Top    booleano quando
                                            Critic quando       aplicável
                                            aplicável.          
  ------------------------------------------------------------------------------

#### `prata.avaliacoes_harmonizadas`

Contrato analítico comum das avaliações IMDb e Rotten Tomatoes,
preservando fonte e semântica original.

  ------------------------------------------------------------------------------
  Campo                      Tipo           Descrição           Domínio /
                             Databricks                         observação
  -------------------------- -------------- ------------------- ----------------
  `id_avaliacao_global`      `STRING`       Identificador       Conforme a fonte
                                            técnico global e    ou o tipo do
                                            único da avaliação  campo; sem
                                            harmonizada.        domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `id_avaliacao`             `STRING`       Identificador       Conforme a fonte
                                            técnico da          ou o tipo do
                                            avaliação.          campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `fonte`                    `STRING`       Fonte da avaliação. Conforme a fonte
                                                                ou o tipo do
                                                                campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `tipo_avaliador`           `STRING`       Tipo de avaliador,  Conforme a fonte
                                            quando aplicável.   ou o tipo do
                                                                campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `id_filme`                 `STRING`       Identificador       Conforme a fonte
                                            técnico do filme.   ou o tipo do
                                                                campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `titulo_filme`             `STRING`       Título do filme     Conforme a fonte
                                            harmonizado.        ou o tipo do
                                                                campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `link_filme`               `STRING`       Referência ao filme Conforme a fonte
                                            na fonte, quando    ou o tipo do
                                            aplicável.          campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `texto_avaliacao`          `STRING`       Texto da avaliação  Conforme a fonte
                                            no contrato         ou o tipo do
                                            harmonizado.        campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `classificacao_original`   `STRING`       Classificação       Conforme a fonte
                                            original preservada ou o tipo do
                                            da fonte.           campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `sentimento`               `STRING`       Sentimento          `POSITIVO` ou
                                            harmonizado.        `NEGATIVO`

  `nota_original`            `STRING`       Nota original       Conforme a fonte
                                            preservada sem      ou o tipo do
                                            normalização        campo; sem
                                            artificial.         domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `data_avaliacao`           `DATE`         Data da avaliação   Conforme a fonte
                                            quando disponível.  ou o tipo do
                                                                campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `nome_avaliador`           `STRING`       Nome do             Conforme a fonte
                                            avaliador/crítico   ou o tipo do
                                            quando disponível.  campo; sem
                                                                domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `publicacao`               `STRING`       Publicação          Conforme a fonte
                                            associada ao        ou o tipo do
                                            crítico quando      campo; sem
                                            disponível.         domínio
                                                                categórico
                                                                adicional
                                                                documentado.

  `eh_top_critic`            `BOOLEAN`      Indicador de Top    booleano quando
                                            Critic quando       aplicável
                                            aplicável.          

  `data_hora_integracao`     `TIMESTAMP`    Campo preservado ou Conforme a fonte
                                            derivado conforme o ou o tipo do
                                            contrato da tabela  campo; sem
                                            e a fonte de        domínio
                                            origem.             categórico
                                                                adicional
                                                                documentado.
  ------------------------------------------------------------------------------

#### `prata.imdb_avaliacoes`

Avaliações IMDb limpas, deduplicadas, identificadas e com sentimento
harmonizado.

  ----------------------------------------------------------------------------
  Campo                       Tipo Databricks Descrição        Domínio /
                                                               observação
  --------------------------- --------------- ---------------- ---------------
  `id_avaliacao`              `STRING`        Identificador    Conforme a
                                              técnico da       fonte ou o tipo
                                              avaliação.       do campo; sem
                                                               domínio
                                                               categórico
                                                               adicional
                                                               documentado.

  `review`                    `STRING`        Texto da         Conforme a
                                              avaliação IMDb.  fonte ou o tipo
                                                               do campo; sem
                                                               domínio
                                                               categórico
                                                               adicional
                                                               documentado.

  `sentiment`                 `STRING`        Classificação    `positive` ou
                                              original de      `negative`
                                              sentimento do    (IMDb)
                                              IMDb.            

  `sentimento`                `STRING`        Sentimento       `POSITIVO` ou
                                              harmonizado.     `NEGATIVO`

  `fonte_dados`               `STRING`        Fonte de dados.  Conforme a
                                                               fonte ou o tipo
                                                               do campo; sem
                                                               domínio
                                                               categórico
                                                               adicional
                                                               documentado.

  `data_hora_processamento`   `TIMESTAMP`     Data/hora        Conforme a
                                              técnica do       fonte ou o tipo
                                              processamento.   do campo; sem
                                                               domínio
                                                               categórico
                                                               adicional
                                                               documentado.
  ----------------------------------------------------------------------------

#### `prata.rt_avaliacoes`

Críticas Rotten Tomatoes limpas, deduplicadas, tipadas e com sentimento
harmonizado.

  ------------------------------------------------------------------------------
  Campo                       Tipo           Descrição            Domínio /
                              Databricks                          observação
  --------------------------- -------------- -------------------- --------------
  `id_avaliacao`              `STRING`       Identificador        Conforme a
                                             técnico da           fonte ou o
                                             avaliação.           tipo do campo;
                                                                  sem domínio
                                                                  categórico
                                                                  adicional
                                                                  documentado.

  `rotten_tomatoes_link`      `STRING`       Identificador/link   Conforme a
                                             do filme no Rotten   fonte ou o
                                             Tomatoes.            tipo do campo;
                                                                  sem domínio
                                                                  categórico
                                                                  adicional
                                                                  documentado.

  `critic_name`               `STRING`       Nome do crítico.     Conforme a
                                                                  fonte ou o
                                                                  tipo do campo;
                                                                  sem domínio
                                                                  categórico
                                                                  adicional
                                                                  documentado.

  `top_critic`                `STRING`       Indicador de Top     Conforme a
                                             Critic.              fonte ou o
                                                                  tipo do campo;
                                                                  sem domínio
                                                                  categórico
                                                                  adicional
                                                                  documentado.

  `eh_top_critic`             `BOOLEAN`      Indicador de Top     booleano
                                             Critic quando        quando
                                             aplicável.           aplicável

  `publisher_name`            `STRING`       Publicação associada Conforme a
                                             ao crítico.          fonte ou o
                                                                  tipo do campo;
                                                                  sem domínio
                                                                  categórico
                                                                  adicional
                                                                  documentado.

  `review_type`               `STRING`       Classificação        `Fresh` ou
                                             original da crítica  `Rotten` (RT)
                                             Rotten Tomatoes.     

  `sentimento`                `STRING`       Sentimento           `POSITIVO` ou
                                             harmonizado.         `NEGATIVO`

  `review_score`              `STRING`       Nota original da     Conforme a
                                             crítica, preservada  fonte ou o
                                             no formato da fonte. tipo do campo;
                                                                  sem domínio
                                                                  categórico
                                                                  adicional
                                                                  documentado.

  `review_date`               `STRING`       Data original da     Conforme a
                                             avaliação.           fonte ou o
                                                                  tipo do campo;
                                                                  sem domínio
                                                                  categórico
                                                                  adicional
                                                                  documentado.

  `data_avaliacao`            `DATE`         Data da avaliação    Conforme a
                                             quando disponível.   fonte ou o
                                                                  tipo do campo;
                                                                  sem domínio
                                                                  categórico
                                                                  adicional
                                                                  documentado.

  `review_content`            `STRING`       Conteúdo textual da  Conforme a
                                             crítica.             fonte ou o
                                                                  tipo do campo;
                                                                  sem domínio
                                                                  categórico
                                                                  adicional
                                                                  documentado.

  `fonte_dados`               `STRING`       Fonte de dados.      Conforme a
                                                                  fonte ou o
                                                                  tipo do campo;
                                                                  sem domínio
                                                                  categórico
                                                                  adicional
                                                                  documentado.

  `data_hora_processamento`   `TIMESTAMP`    Data/hora técnica do Conforme a
                                             processamento.       fonte ou o
                                                                  tipo do campo;
                                                                  sem domínio
                                                                  categórico
                                                                  adicional
                                                                  documentado.
  ------------------------------------------------------------------------------

#### `prata.rt_filmes`

Dados de filmes Rotten Tomatoes tratados, tipados e identificados.

  --------------------------------------------------------------------------------------
  Campo                                Tipo          Descrição            Domínio /
                                       Databricks                         observação
  ------------------------------------ ------------- -------------------- --------------
  `id_filme`                           `STRING`      Identificador        Conforme a
                                                     técnico do filme.    fonte ou o
                                                                          tipo do campo;
                                                                          sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `rotten_tomatoes_link`               `STRING`      Identificador/link   Conforme a
                                                     do filme no Rotten   fonte ou o
                                                     Tomatoes.            tipo do campo;
                                                                          sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `movie_title`                        `STRING`      Título do filme.     Conforme a
                                                                          fonte ou o
                                                                          tipo do campo;
                                                                          sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `data_lancamento_original`           `DATE`        Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `data_lancamento_streaming`          `DATE`        Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `duracao_minutos`                    `INT`         Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `avaliacao_tomatometer`              `DOUBLE`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `quantidade_tomatometer`             `LONG`        Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `avaliacao_audiencia`                `DOUBLE`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `quantidade_audiencia`               `LONG`        Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `movie_info`                         `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `critics_consensus`                  `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `content_rating`                     `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `genres`                             `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `directors`                          `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `authors`                            `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `actors`                             `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `original_release_date`              `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `streaming_release_date`             `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `runtime`                            `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `production_company`                 `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `tomatometer_status`                 `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `tomatometer_rating`                 `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `tomatometer_count`                  `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `audience_status`                    `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `audience_rating`                    `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `audience_count`                     `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `tomatometer_top_critics_count`      `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `tomatometer_fresh_critics_count`    `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `tomatometer_rotten_critics_count`   `STRING`      Campo preservado ou  Conforme a
                                                     derivado conforme o  fonte ou o
                                                     contrato da tabela e tipo do campo;
                                                     a fonte de origem.   sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `fonte_dados`                        `STRING`      Fonte de dados.      Conforme a
                                                                          fonte ou o
                                                                          tipo do campo;
                                                                          sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.

  `data_hora_processamento`            `TIMESTAMP`   Data/hora técnica do Conforme a
                                                     processamento.       fonte ou o
                                                                          tipo do campo;
                                                                          sem domínio
                                                                          categórico
                                                                          adicional
                                                                          documentado.
  --------------------------------------------------------------------------------------

### 4.3 Linhagem dos dados

``` text
Kaggle
  ↓
Dados Brutos / Landing Zone
  ↓
Bronze
  ↓
Prata por fonte
  ↓
prata.avaliacoes_harmonizadas
  ↓
Ouro: dimensões + fato_avaliacao
  ↓
Análises de negócio
```

Na Bronze, os campos originais são preservados e são acrescentados
metadados técnicos como `_fonte`, `_arquivo_origem` e `_data_ingestao`,
conforme aplicável. Na Prata ocorrem limpeza, tipagem, deduplicação,
criação de identificadores e harmonização. Na Ouro os dados são
organizados dimensionalmente para consumo analítico.

  --------------------------------------------------------------------------------------
            5\. Pipe line de Dados (Etapa 4.4)            abilidade única.
  ------------------ ------------------------------------ ------------------------------
   06 07 08 09 10 11 `01_ingestao_dados_brutos`           Coleta e Landing Zone
                  12 `02_camada_bronze`                   Construção da Bronze Profiling
                     `03_profiling_qualidade_bronze`      inicial Tratamento IMDb
                     `04_imdb_prata` `05_rt_filmes_prata` Tratamento dos filmes RT
                     `06_rt_avaliacoes_prata`             Tratamento das críticas RT
                     `07_integracao_harmonizacao_prata`   Harmonização das fontes
                     `08_profiling_qualidade_prata`       Qualidade da Prata Dimensões
                     `09_dimensoes_ouro`                  Tabela fato Qualidade da Ouro
                     `10_fato_avaliacao_ouro`             Respostas analíticas
                     `11_validacao_qualidade_ouro`        
                     `12_analises_negocio`                

             tabelas foram persistidas em **Delta Lake**. 

   opção po ansforma r notebooks separados tornou         o o fluxo entre ingestão, além
            speção d explícit ção, validação, modelagem e de facilitar a
                     consumo, os impactos de cada etapa.  

            Evidênci a a disponibilizar:\*\*              

    `markdo Tabelas` wn persistidas no                    /05_tabelas_persistidas.png)
                     Databricks\](evidencias              
  --------------------------------------------------------------------------------------

## 6. Qualidade de Dados (Etapa 4.5)

A qualidade foi avaliada antes das análises, considerando completude,
consistência, unicidade, plausibilidade e valores extremos, além de
integridade referencial.

### 6.1 IMDb

  Indicador                       Resultado
  ----------------------------- -----------
  Registros Bronze                   50.000
  Registros Prata                    49.582
  Duplicidades removidas                418
  Textos nulos                            0
  Sentimentos nulos                       0
  Sentimentos fora do domínio             0
  IDs distintos                      49.582

### 6.2 Rotten Tomatoes --- críticas

  Indicador                                    Resultado
  ------------------------------------------ -----------
  Registros Bronze                             1.130.017
  Registros Prata                              1.010.546
  Duplicidades removidas                         119.471
  IDs distintos                                1.010.546
  Links distintos sem filme correspondente             6
  Avaliações associadas a esses links                130

O campo `review_score` apresentou heterogeneidade de formatos. Entre os
**736.941 valores preenchidos**, foram observadas principalmente frações
e notas por letras, além de poucos valores puramente numéricos. Outros
**273.605 registros** não possuíam nota preenchida.

A decisão foi preservar `review_score`/`nota_original` como texto,
evitando impor uma escala numérica artificial que alterasse o
significado original.

### 6.3 Harmonização

A tabela harmonizada apresentou:

-   **1.060.128 registros**;
-   **1.060.128 IDs globais distintos**;
-   **0 IDs globais duplicados**;
-   **0 sentimentos fora do domínio**;
-   `texto_avaliacao` preenchido em **94,48%** dos registros;
-   **850 textos com menos de 10 caracteres**, mantidos e documentados.

### 6.4 Validação da Ouro

  Verificação               Resultado
  ----------------------- -----------
  Prata harmonizada         1.060.128
  Fato Ouro                 1.060.128
  IDs globais distintos     1.060.128
  Órfãos de fonte                   0
  Órfãos de filme                   0
  Órfãos de data                    0
  Sem `id_fonte`                    0
  Sem `id_filme`               49.712
  Sem `id_data`                49.582
  Cobertura de filme           95,31%
  Cobertura de data            95,32%

Os registros sem filme ou data não foram descartados. No caso do IMDb, a
ausência dessas dimensões decorre da própria estrutura da fonte
utilizada. Preservar esses registros foi uma decisão deliberada para não
criar dados inexistentes nem perder avaliações válidas.

``` markdown
```

------------------------------------------------------------------------

## 7. Análise de Dados (Etapa 4.5)

### 7.1 Pergunta 1 --- Qual é a distribuição de sentimento em cada fonte?

  Fonte             Sentimento     Quantidade   Percentual
  ----------------- ------------ ------------ ------------
  IMDb              NEGATIVO           24.698       49,81%
  IMDb              POSITIVO           24.884       50,19%
  Rotten Tomatoes   NEGATIVO          366.614       36,28%
  Rotten Tomatoes   POSITIVO          643.932       63,72%

O IMDb ficou praticamente equilibrado. No Rotten Tomatoes, as avaliações
positivas predominam.

**Interpretação:** a diferença reforça a necessidade de preservar a
variável `fonte`. A integração técnica não significa que os dois
datasets devam ser interpretados como uma única população homogênea.

### 7.2 Pergunta 2 --- Existem diferenças entre as proporções observadas nas fontes?

Sim, descritivamente. O IMDb possui **50,19%** de avaliações positivas,
enquanto o Rotten Tomatoes possui **63,72%**.

A diferença é de **13,53 pontos percentuais**. Este MVP descreve a
diferença, mas não atribui causalidade a ela, pois as fontes possuem
naturezas, processos de classificação e volumes diferentes.

O resultado global --- **668.816 avaliações positivas (63,09%)** e
**391.312 negativas (36,91%)** --- é fortemente influenciado pelo maior
volume do Rotten Tomatoes.

### 7.3 Pergunta 3 --- Top Critics apresentam comportamento diferente?

  Categoria         Sentimento     Quantidade   Percentual
  ----------------- ------------ ------------ ------------
  Demais críticos   NEGATIVO          260.827       34,68%
  Demais críticos   POSITIVO          491.348       65,32%
  Top Critic        NEGATIVO          105.787       40,94%
  Top Critic        POSITIVO          152.584       59,06%

Os Top Critics apresentam proporção positiva **6,26 pontos percentuais
menor** que os demais críticos.

Novamente, o resultado é descritivo: ele identifica um padrão no
conjunto analisado, sem concluir por que esse padrão ocorre.

### 7.4 Pergunta 4 --- Como o sentimento se distribui ao longo do tempo?

A análise temporal foi realizada para o Rotten Tomatoes, pois a fonte
possui data de avaliação.

Foram identificadas combinações de ano e sentimento ao longo do período
disponível. Os anos iniciais apresentam volumes muito pequenos,
inclusive casos com apenas uma ou poucas avaliações. Percentuais de 100%
nesses anos, portanto, não devem ser interpretados com o mesmo peso de
períodos com maior volume.

A principal conclusão é metodológica: **percentual e quantidade precisam
ser observados conjuntamente** na análise temporal.

### 7.5 Pergunta 5 --- Quais filmes concentram maior quantidade de avaliações?

Entre os primeiros colocados:

  Filme                             Avaliações
  ------------------------------- ------------
  Joker                                    574
  Once Upon a Time In Hollywood            554
  Us                                       535
  Avengers: Endgame                        528
  Captain Marvel                           523
  A Star Is Born                           517
  Black Panther                            512

### 7.6 Perguntas 6 e 7 --- Como comparar positividade entre filmes sem depender de amostras pequenas?

A distribuição de avaliações entre **17.706 filmes** apresentou:

  Estatística      Avaliações
  -------------- ------------
  Mínimo                    1
  Percentil 25             12
  Mediana                  28
  Percentil 75             75
  Percentil 90            154
  Máximo                  574

Em vez de escolher arbitrariamente um número mínimo de avaliações, foi
utilizado o **percentil 75**, equivalente a **75 avaliações**, como
limiar para a comparação de positividade.

Com esse critério, **4.469 filmes** permaneceram elegíveis.

Entre os títulos exibidos nas primeiras posições da análise de proporção
positiva estão *Paddington 2*, *Leave No Trace*, *Toy Story 2*, *Man on
Wire*, *Taxi to the Dark Side*, *Citizen Kane*, *Toy Story* e *Finding
Nemo*.

O critério não transforma a análise em inferência estatística, mas reduz
a fragilidade de rankings baseados em pouquíssimas observações.

### 7.7 Discussão geral

O pipeline permitiu transformar fontes heterogêneas em uma estrutura
comum sem apagar suas diferenças.

O resultado mais importante do ponto de vista do problema original é que
**preservar semântica e preservar origem foram decisões tão importantes
quanto limpar e integrar os dados**.

A assimetria entre os datasets, conhecida desde o início, tornou-se um
elemento central da análise: se a origem tivesse sido descartada durante
a harmonização, o resultado agregado de 63,09% de avaliações positivas
poderia induzir à interpretação equivocada de que ele representa
igualmente IMDb e Rotten Tomatoes.

A separação entre classificação original e sentimento harmonizado
permitiu combinar os dados e, simultaneamente, manter o caminho de volta
para o significado da fonte.

``` markdown
```

------------------------------------------------------------------------

## 8. Autoavaliação

### 8.1 Atingimento dos objetivos

Considero que o MVP atingiu o objetivo proposto.

Foi construída uma pipeline de ponta a ponta em ambiente de nuvem, desde
a obtenção dos arquivos públicos até a disponibilização de um modelo
dimensional para análise.

Além do aprendizado técnico, o trabalho permitiu explorar um tema de
interesse pessoal. O uso de dados de cinema e crítica cinematográfica
tornou mais concreta a preocupação com a semântica: uma opinião não
deveria perder sua origem ou ser transformada apenas para tornar as
tabelas mais convenientes.

Essa preocupação influenciou decisões como preservar classificações
originais, não normalizar artificialmente notas heterogêneas e manter
registros sem dimensões que não existiam na fonte.

### 8.2 Principais dificuldades

As principais dificuldades foram:

-   integrar fontes com estruturas e granularidades diferentes;
-   trabalhar com uma diferença superior a uma ordem de grandeza entre
    os volumes de avaliações;
-   deduplicar sem eliminar informação válida;
-   construir identificadores técnicos estáveis;
-   preservar a relação entre crítica e filme;
-   lidar com notas de críticos em formatos heterogêneos;
-   harmonizar `positive`/`negative` e `Fresh`/`Rotten` sem tratar as
    fontes como metodologicamente equivalentes;
-   manter registros sem filme ou data quando essas informações não
    existiam ou não eram aplicáveis;
-   preservar a granularidade da Prata ao construir a fato na Ouro;
-   adaptar a implementação às características do Databricks Free
    Edition/Serverless.

A diferença de volume entre IMDb e Rotten Tomatoes merece destaque. Ela
era conhecida antes da implementação, mas tornou-se um desafio efetivo
do MVP porque afetava tanto a arquitetura quanto a interpretação. A
solução adotada foi **não balancear artificialmente as fontes**, manter
sua identificação em todas as etapas relevantes e privilegiar análises
estratificadas por fonte.

### 8.3 Aprendizados

O principal aprendizado foi compreender que um pipeline não é apenas uma
sequência de transformações.

Decisões aparentemente simples --- eliminar um campo, converter uma
nota, preencher uma data ausente ou juntar duas classificações --- podem
alterar o significado do dado.

Neste projeto, Engenharia de Dados significou também estabelecer
contratos, documentar limitações, preservar rastreabilidade e tornar
explícito quando dois valores podem ser harmonizados para análise sem
afirmar que são equivalentes em sua origem.

### 8.4 Trabalhos futuros

Possíveis evoluções:

-   aplicar NLP ao texto das avaliações;
-   comparar o sentimento original com sentimento estimado por modelos
    de linguagem;
-   analisar intensidade e vocabulário das críticas, além da polaridade
    binária;
-   explorar diferenças entre publicações e críticos;
-   enriquecer filmes com gênero, direção, elenco e premiações;
-   criar dashboards no Databricks;
-   automatizar orquestração e testes de qualidade;
-   implementar processamento incremental;
-   investigar técnicas de ponderação que permitam comparar fontes com
    volumes muito diferentes sem apagar a distribuição original.

------------------------------------------------------------------------

## 9. Reprodutibilidade e Organização do Repositório

Estrutura prevista:

``` text
MVP3_PUCRio_Data_Pipeline/
│
├── README.md
├── notebooks/
│   ├── 01_ingestao_dados_brutos
│   ├── 02_camada_bronze
│   ├── 03_profiling_qualidade_bronze
│   ├── 04_imdb_prata
│   ├── 05_rt_filmes_prata
│   ├── 06_rt_avaliacoes_prata
│   ├── 07_integracao_harmonizacao_prata
│   ├── 08_profiling_qualidade_prata
│   ├── 09_dimensoes_ouro
│   ├── 10_fato_avaliacao_ouro
│   ├── 11_validacao_qualidade_ouro
│   └── 12_analises_negocio
```

**Repositório público GitHub:**
https://github.com/fagundespereiraGIT25/MVP3_PUCRio_Data_Pipeline

Os dados brutos não são versionados no GitHub. A reprodução é feita a
partir das fontes documentadas e do notebook de ingestão.

------------------------------------------------------------------------

## 10. Conclusão

Este MVP resultou em um pipeline funcional de Engenharia de Dados na
nuvem, construído no Databricks e capaz de integrar mais de um milhão de
avaliações cinematográficas.

A experiência permitiu trabalhar simultaneamente um objetivo técnico e
um tema de interesse pessoal. Do ponto de vista técnico, o projeto
percorreu coleta, armazenamento, Bronze, Prata, Ouro, qualidade,
modelagem dimensional e análise. Do ponto de vista do domínio, permitiu
observar como opiniões sobre filmes podem ser estruturadas sem reduzir
toda a riqueza da origem a um único número ou rótulo.

A diferença de escala entre IMDb e Rotten Tomatoes, conhecida desde a
seleção das fontes, mostrou-se particularmente instrutiva. Ela
demonstrou que integrar dados não significa necessariamente torná-los
iguais: em muitos casos, a melhor solução é preservar diferenças,
documentá-las e construir um modelo que permita analisá-las
conscientemente.

A camada Ouro preservou os **1.060.128 registros** da Prata harmonizada,
com identificadores únicos e integridade referencial validada. As
perguntas de negócio puderam ser respondidas e as limitações dos dados
foram explicitadas.

Assim, o trabalho cumpre a finalidade central do MVP: transformar dados
brutos em dados organizados e capazes de gerar respostas, mantendo a
preocupação com qualidade, rastreabilidade e significado.

------------------------------------------------------------------------

## 11. Referências e Fontes

### Dataset IMDb

**IMDb Dataset of 50K Movie Reviews --- 50 mil avaliações de filmes para
classificação binária de sentimento**\
Kaggle --- `lakshmi25npathi/imdb-dataset-of-50k-movie-reviews`\
https://www.kaggle.com/datasets/lakshmi25npathi/imdb-dataset-of-50k-movie-reviews

### Dataset Rotten Tomatoes

**Rotten Tomatoes Movies and Critic Reviews Dataset --- filmes e
avaliações de críticos profissionais**\
Kaggle ---
`stefanoleone992/rotten-tomatoes-movies-and-critic-reviews-dataset`\
https://www.kaggle.com/datasets/stefanoleone992/rotten-tomatoes-movies-and-critic-reviews-dataset

------------------------------------------------------------------------
