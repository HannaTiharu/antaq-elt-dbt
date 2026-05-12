
# 🚢 ​Sazonalidade e Climatologia de Atracação (ANTAQ)



Este projeto utiliza engenharia de dados para analisar a **sazonalidade do tempo de espera** para atracação de navios no Brasil, utilizando dados brutos da ANTAQ (2010-2026). O objetivo central é facilitar a correlação entre a eficiência operacional dos portos e os ciclos das principais commodities agrícolas brasileiras.

## 🎯 Objetivo
O projeto visa transformar registros complexos de atracação em métricas sazonais claras. A análise permite identificar como o escoamento de safras (como soja, milho e cana-de-açúcar) impacta o *Lead Time* de outras operações, ajudando na tomada de decisão estratégica sobre "por onde entrar" no Brasil em diferentes meses do ano.

*   **Padrão Histórico como Preditor:** Assim como na meteorologia, a climatologia logística permite prever janelas de eficiência baseadas em ciclos recorrentes.
*   **Correlação Estrutural:** Identifica que o "vale" de espera no Nordeste coincide sistematicamente com a entressafra da cana, transformando dados passados em ferramenta de predição.
*   **Redução de Ruído:** O uso de médias sazonais históricas filtra anomalias pontuais, focando no comportamento estrutural da matriz de transporte brasileira.


## 📂 Dados e Origem
Os dados brutos são extraídos do portal de **Dados Abertos da ANTAQ**:
🔗 [Portal de Dados Abertos ANTAQ](https://www.gov.br/antaq/pt-br/acesso-a-informacao/dados-abertos)

Para a execução do pipeline, são necessários os arquivos anuais de:
1.  **Atracação:** Registros de tempos de chegada, início e fim de operação.
2.  **Carga:** Detalhamento das mercadorias (essencial para correlação com commodities).

**Estrutura de pastas esperada:**
```texto
/data
  ├── atracacao_ANO.csv
  └── carga_ANO.csv
```
  

## 🛠️ Stack Tecnológica e Arquitetura
O pipeline foi construído utilizando práticas modernas de **Analytics Engineering**:
*   **Extração:** Dados Abertos da ANTAQ (2010-2026).
*   **Transformação (ELT):** `dbt` (data build tool) seguindo a **Arquitetura Medallion**:
    *   **Staging:** Limpeza, padronização e tipagem dos dados brutos.
    *   **Intermediate:** Aplicação de lógica de negócio e cálculos de intervalos de tempo.
    *   **Mart:** Tabelas agregadas por porto e mês, otimizadas para análise de sazonalidade.
*   **Visualização:** Looker Studio / Data Studio para exploração visual das correlações.


## 🛠️ Tratamento de Dados e Regras de Negócio

Para garantir a integridade da **Climatologia de Dados**, foram aplicadas camadas de tratamento e filtragem utilizando SQL no dbt:

### 1. Limpeza e Qualidade (Data Cleaning)
*   **Remoção de Nulos:** Filtragem de registros onde a identificação do porto era inexistente.
*   **Tratamento de Outliers:** Estabeleceu-se um limite de **2.160 horas (90 dias)** para o tempo de espera. Registros acima deste teto foram descartados para evitar que navios parados para reparos de longuíssimo prazo ou erros de preenchimento distorcessem as médias.

### 2. Segmentação e Relevância Estatística
*   **Foco Comercial:** Os dados foram categorizados por tipo de operação. A análise concentra-se exclusivamente na categoria **"Comercial"** (Movimentação de Carga e Misto), descartando operações de apoio, passageiros ou manutenção.
*   **Significância Histórica:** Para evitar o viés de portos com dados sazonais incompletos, foram filtrados apenas os portos que apresentaram pelo menos **10 anos de histórico** de movimentação na base da ANTAQ.

### 3. Engenharia de Atributos e Métricas
*   **Cálculo de Espera:** Transformação da diferença entre Data de Chegada e Data de Atracação em horas decimais.
*   **Cálculo do Índice de Sazonalidade:** 
    *   **Média Mensal:** Calculada por porto ao longo de todo o histórico.
    *   **Média Global:** Média total de espera do porto em todo o período analisado.
    *   **Índice:** Razão entre a Média Mensal e a Média Global ($m.media\_mensal / g.media\_global$). 
    > *Este índice permite identificar se um mês específico está acima ou abaixo da "normalidade" operacional daquele porto.*


## 📈 Principais Insights
*   **Média Nacional:** O tempo médio de espera para atracação identificado foi de **42,4 horas**.
*   **Divergência Regional (Junho - Setembro):** 
    *   **Sul/Sudeste:** Portos como Santos e Paranaguá atingem picos de espera, refletindo a confluência das safras de soja e milho-safrinha.
    *   **Nordeste:** No mesmo período, portos como Maceió e Recife apresentam vales de sazonalidade (devido à entressafra da cana-de-açúcar), oferecendo uma "via expressa" para operações de importação.


## 🚀 Como Explorar
1.  Certifique-se de que os arquivos da ANTAQ estejam na pasta `/data`.
2.  Execute o projeto dbt para gerar as tabelas:
    ```bash
    dbt run
    dbt test
    ```
3.  **Dashboard Interativo:** [https://datastudio.google.com/reporting/e2516eb3-c47a-44ee-af2c-5e86b595e320]

### 🎥 Demonstração do Dashboard
[Demonstração do Dashboard Portuário - ANTAQ](

https://github.com/user-attachments/assets/48d90ca3-ec23-4cdb-af62-010395b6ca9d

)

---
**Desenvolvido por Hanna Tiharu Kodama**


