import functools
import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import Table, create_engine, inspect, MetaData, text
from sqlalchemy.orm import sessionmaker
import requests
from llama_index.core.retrievers import NLSQLRetriever
from llama_index.core.indices.struct_store import SQLTableRetrieverQueryEngine
from llama_index.core import VectorStoreIndex
from llama_index.core.objects import (
    ObjectIndex,
    SQLTableNodeMapping,
    SQLTableSchema,
)
from llama_index.llms.openai import OpenAI
from llama_index.legacy import SQLDatabase

import logging

from settings import settings

# Set up logging
log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

router = APIRouter()

DB_PASSWORD = settings.REPORTING_PASSWORD
DB_HOST_PORT = settings.REPORTING_HOST
DB = settings.REPORTING_DB
USER = settings.REPORTING_USER

# Construct the connection string
SQL_DATABASE_URL = f'mssql+pymssql://{USER}:{DB_PASSWORD}@{DB_HOST_PORT}/{DB}'

# Create an engine instance
engine = create_engine(
    SQL_DATABASE_URL, connect_args={}, echo=False
)
metadata = MetaData()
metadata.reflect(bind=engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
inspector = inspect(engine)


# OpenAI setup
os.environ["OPENAI_API_KEY"] = settings.OPENAI_KEY
llm = OpenAI(temperature=0, model="gpt-3.5-turbo")

# Database setup
tables = ["Linelist_FACTART", "LineListTransHTS", "LinelistPrep", "LinelistPrepAssessments", "LinelistHEI",
          "LinelistHTSEligibilty", "LineListOVCEligibilityAndEnrollments", "LineListOTZEligibilityAndEnrollments",
          "LineListPBFW", "LineListTransPNS"]
sql_database = SQLDatabase(engine, include_tables=tables)
CACHE_TIMEOUT = 3600  # 1 hour


def get_dictionary_info():
    # JWT and host for fetching table descriptions
    jwt_token = settings.OM_JWT
    om_host = settings.OM_HOST

    # Fetch table descriptions and metadata
    tables_info = []
    for table_name in tables:
        table_description = ""
        columns_info = {}
        table_glossary_uri = f"{om_host}/api/v1/glossaryTerms/name/text2sql.{table_name}"
        try:
            response = requests.get(table_glossary_uri, headers={"Authorization": "Bearer " + jwt_token}, verify=False)
            response.raise_for_status()

            if response.status_code // 100 == 2:
                glossary_term = response.json()
                table_description = glossary_term.get("description")
                if table_description:
                    table = Table(table_name, metadata, autoload=True)
                    columns = table.columns.keys()
                    columns_info_list = []
                    for column_name in columns:
                        column_glossary_uri = f"{om_host}/api/v1/glossaryTerms/name/text2sql.{table_name}.{column_name}"
                        try:
                            response = requests.get(column_glossary_uri,
                                                    headers={"Authorization": "Bearer " + jwt_token}, verify=False)
                            response.raise_for_status()

                            if response.status_code // 100 == 2:
                                column_info = response.json()
                                column_desc = f"{column_name}: {column_info.get('description')}"
                                columns_info_list.append(column_desc)
                        except requests.exceptions.HTTPError as he:
                            if he.response.status_code // 100 == 4:
                                print(
                                    f"Glossary term not found for URI: {column_glossary_uri}")
                            else:
                                print(
                                    f"Failed to retrieve column description for {column_name} with message {he.response.text}",
                                    he)
                    columns_info = ". ".join(columns_info_list)
        except requests.exceptions.HTTPError as he:
            if he.response.status_code // 100 == 4:
                print(f"Glossary term not found for URI: {table_glossary_uri}")
            else:
                print(
                    f"Failed to retrieve table description for {table_name} with message {he.response.text}", he)

        tables_info.append(SQLTableSchema(
            table_name=table_name,
            context_str=(table_description + '. These are columns in the table: ' + columns_info)
        ))

    return tables_info


@functools.lru_cache(maxsize=None)
def get_dictionary_info_cached():
    return get_dictionary_info()


# Endpoint to retrieve data based on natural language query
class NaturalLanguageQuery(BaseModel):
    question: str


@router.post('/query_from_natural_language')
async def query_from_natural_language(nl_query: NaturalLanguageQuery):
    try:
        log.debug(f"Received query: {nl_query.question}")

        # Create Object Index and Query Engine
        table_node_mapping = SQLTableNodeMapping(sql_database)
        obj_index = ObjectIndex.from_objects(
            get_dictionary_info_cached(),
            table_node_mapping,
            VectorStoreIndex,
        )
        query_engine = SQLTableRetrieverQueryEngine(
            sql_database,
            obj_index.as_retriever(similarity_top_k=2),
        )
        nl_sql_retriever = NLSQLRetriever(
            sql_database,
            table_retriever=obj_index.as_retriever(similarity_top_k=2),
            # sql_only=True,
        )

        custom_txt2sql_prompt = """Given an input question, construct a syntactically correct SQL query to run, then look at the results of the query and return a comprehensive and detailed answer. Ensure that you:
                    - Select only the relevant columns needed to answer the question.
                    - Use correct column and table names as provided in the schema description. Avoid querying for columns that do not exist.
                    - Qualify column names with the table name when necessary, especially when performing joins.
                    - Use aggregate functions appropriately and include performance optimizations such as WHERE clauses and indices.
                    - Add additional related information for the user.
                    - Use background & definitions provided for more detailed answer. Follow the instructions.
                    - Avoid hallucination. If you can't find an answer, say I'm not sure.


                     Special Instructions:
                    - Treat "txcurr" and "active patients on treatment" as interchangeable terms in your queries.
                    - HIV Exposed Infants (HEI) and HEI are used interchangably.
                    - Default to using averages for aggregation if not specified by the user question.
                    - If the question involves a KPI not listed below, inform the user by showing the list of available KPIs.
                    - If the requested date range is not available in the database, inform the user that data is not available for that time period.
                    - Use bold and large fonts to highlight keywords in your answer.
                    - If the date is not available, your answer will be: Data is not available for the requested date range. Please modify your query to include a valid date range.
                    - Calculate date ranges dynamically based on the current date or specific dates mentioned in user queries. Use relative time expressions such as "last month" or "past year".
                    - If a query fails to execute, suggest debugging tips or provide alternative queries. Ensure to handle common SQL errors gracefully."
                    - If the query is ambiguous, generate a clarifying question to better understand the user's intent or request additional necessary parameters.
                    - Use indexed columns for joins and WHERE clauses to speed up query execution. Use `EXPLAIN` plans for complex queries to ensure optimal performance.
                    - Join ON "PatientPKHASH" AND "MFLCode" for queries that require joins.
                    - txcurr is where IsTxcurr=1. "
                    - When NUPI = NULL means you have not been verified"
                    - To get unsuppressed clients first filter those with a Valid VL, then check if they are Unsuppressed."
                    - Get HIV risks(low risk, medium risk, high risk, very high risk) from the LinelistHTSEligibilty table in HIVRiskCategory column "
                    - To calculate Positivity rate get the number of positive tests from finaltestresults and divide by is Total number of tests (Positives and Negatives)"
                    - To calculate unsuppression/non-suppression rate, Numerator is valid vl(HasValidVL)and unsuppressed(Validvlsup=0) and the denominator is valid vl (HasValidvl=1)."
                    - To calculate suppression rate, Numerator is valid suppressed(Validvlsup=1) and the denominator is valid vl (HasValidvl=1)."
                    - To calculate IIT rate , Numerator is (ARTOutcomeDescription = Loss to follow up)  and the denominator is (ARTOutcomeDescription= loss to follow up and Active)."
                    - Interruption in treatment (IIT) is the patients with Loss to Follow up outcome description."

                    Additional Instructions:
                    - Encourage users to provide specific date ranges or intervals for more accurate results.
                    - Mention the importance of specifying provinces or regions for targeted analysis.
                    - Provide examples of common SQL syntax errors and how to correct them.
                    - Offer guidance on interpreting query results, including outliers or unexpected patterns.
                    - Emphasize the significance of data integrity and potential implications of incomplete or inaccurate data.
                    - Inform users that data exists from July 2023 for the list of KPIs.

                    You are required to use the following format, each taking one line:

                    Question: Question here
                    SQLQuery: SQL Query to run


                    The text-to-SQL system that might be required to handle queries related to calculating proportions within a dataset. Your system should be able to generate SQL queries to calculate the proportion of a certain category within a dataset table.

                    Instructions:

                    Assume you have a dataset table named Linelist_FACTART containing columns representing different categories.
                    Your system should generate SQL queries that calculate the proportion of a specific category within the table.

                    Example 1 :
                    If a user asks, "What proportion of TxCurr,  have hypertension by county", your system should generate a SQL query like:

                    SELECT County, COUNT(*) AS TotalTxCurr, SUM(CASE WHEN HasHypertension = 1 THEN 1 ELSE 0 END) AS TotalHypertension, SUM(CASE WHEN HasHypertension = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS ProportionHypertension FROM Linelist_FACTART WHERE ISTxCurr = 1 GROUP BY County;

                    Example 2 :
                    If a user asks, "What is the proportion of txcurr who were screened for Hypertensive in the last visit"", your system should generate a SQL query like:

                    SELECT COUNT(*) AS TX_CURR, COUNT(CASE WHEN ScreenedBPLastVisit = 1 THEN 1 END) AS ScreenedBP, COUNT(CASE WHEN ScreenedBPLastVisit = 1 THEN 1 END) * 100.0 / COUNT(*) AS Proportion_ScreenedForHypertension FROM Linelist_FACTART WHERE ISTxCurr = 1;GROUP BY County;

                    Example 3 :
                    If a user asks, "What is unsuppression rate by county", your system should generate a SQL query like:

                    SELECT County, COUNT(*) AS TotalPatientsWithValidVL, COUNT(CASE WHEN HighViremia = 1 THEN 1 END) AS UnsuppressedPatients, COUNT(CASE WHEN HighViremia = 1 THEN 1 END) * 100.0 / COUNT(*) AS UnsuppressionRate FROM Linelist_FACTART WHERE ISTxCurr = 1 AND HasValidVL = 1 GROUP BY County;

                    The text-to-SQL system that might be required to handle queries related to calculating trends or rate of change. Your system should be able to generate SQL queries to calculate trend and increase rate.

                    Instructions:

                    Calculate the rates :Compute Rate Differences: Calculate change in rate(subtract the rate at end period-start period/start period): Multiply the rate difference by 100 to express it as a percentage.

                    Example 1 :
                    If a user asks, "What is the trend in positivity rates across different counties from January 2023 to June 2023, and which county experienced the greatest increase in positivity rate over this period?"", your system should generate a SQL query like:

                    SELECT County, MAX(CASE WHEN Month = 1 THEN PositivityRate END) AS PositivityRate_Month1, MAX(CASE WHEN Month = 2 THEN PositivityRate END) AS PositivityRate_Month2, MAX(CASE WHEN Month = 3 THEN PositivityRate END) AS PositivityRate_Month3, MAX(CASE WHEN Month = 4 THEN PositivityRate END) AS PositivityRate_Month4, MAX(CASE WHEN Month = 5 THEN PositivityRate END) AS PositivityRate_Month5, MAX(CASE WHEN Month = 6 THEN PositivityRate END) AS PositivityRate_Month6, (MAX(CASE WHEN Month = 6 THEN PositivityRate END) - MAX(CASE WHEN Month = 1 THEN PositivityRate END)) AS PositivityRateDifference, CASE WHEN MAX(CASE WHEN Month = 1 THEN PositivityRate END) <> 0 THEN (((MAX(CASE WHEN Month = 6 THEN PositivityRate END) - MAX(CASE WHEN Month = 1 THEN PositivityRate END)) / MAX(CASE WHEN Month = 1 THEN PositivityRate END)) * 100) ELSE NULL END AS RateDifference FROM ( SELECT County, MONTH(TestDate) AS Month, COUNT(CASE WHEN FinalTestResult = 'Positive' THEN 1 END) AS PositiveTests, COUNT(*) AS TotalTests, CASE WHEN COUNT(*) <> 0 THEN COUNT(CASE WHEN FinalTestResult = 'Positive' THEN 1 END) * 100.0 / COUNT(*) ELSE NULL END AS PositivityRate FROM LineListTransHTS WHERE TestDate >= '2023-01-01' AND TestDate < '2023-07-01' GROUP BY County, MONTH(TestDate) ) AS PositivityRates GROUP BY County;
                    when asked for a specific county, Partner or facility , order in descending and filter to the top county/partner/facility, otherwise provide a linelist

                    Use these real examples for complex queries:

                    Example 1:
                    Question: calculate the positivity rate (percentage of positive tests) for each HIV risk category (Very High, High, Medium, Low) within each county
                    SQLQuery: SELECT County, SUM(CASE WHEN HIVRiskCategory = 'Very High' THEN Positives ELSE 0 END) AS VeryHigh_Positives, SUM(CASE WHEN HIVRiskCategory = 'Very High' THEN TotalTests ELSE 0 END) AS VeryHigh_TotalTests, (SUM(CASE WHEN HIVRiskCategory = 'Very High' THEN Positives ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN HIVRiskCategory = 'Very High' THEN TotalTests ELSE 0 END), 0)) AS VeryHigh_PositivityRate, SUM(CASE WHEN HIVRiskCategory = 'High' THEN Positives ELSE 0 END) AS High_Positives, SUM(CASE WHEN HIVRiskCategory = 'High' THEN TotalTests ELSE 0 END) AS High_TotalTests, (SUM(CASE WHEN HIVRiskCategory = 'High' THEN Positives ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN HIVRiskCategory = 'High' THEN TotalTests ELSE 0 END), 0)) AS High_PositivityRate, SUM(CASE WHEN HIVRiskCategory = 'Moderate' THEN Positives ELSE 0 END) AS Moderate_Positives, SUM(CASE WHEN HIVRiskCategory = 'Moderate' THEN TotalTests ELSE 0 END) AS Moderate_TotalTests, (SUM(CASE WHEN HIVRiskCategory = 'Moderate' THEN Positives ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN HIVRiskCategory = 'Moderate' THEN TotalTests ELSE 0 END), 0)) AS Moderate_PositivityRate, SUM(CASE WHEN HIVRiskCategory = 'Low' THEN Positives ELSE 0 END) AS Low_Positives, SUM(CASE WHEN HIVRiskCategory = 'Low' THEN TotalTests ELSE 0 END) AS Low_TotalTests, (SUM(CASE WHEN HIVRiskCategory = 'Low' THEN Positives ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN HIVRiskCategory = 'Low' THEN TotalTests ELSE 0 END), 0)) AS Low_PositivityRate FROM ( SELECT LHTSE.HIVRiskCategory, LHTSE.County, COUNT(CASE WHEN LTFR.FinalTestResult = 'Positive' THEN LTFR.PatientPKHash END) AS Positives, COUNT(LHTSE.PatientPKHash) AS TotalTests FROM LinelistHTSEligibilty LHTSE JOIN LinelistTRANSHTS LTFR ON LHTSE.PatientPKHash = LTFR.PatientPKHash AND LHTSE.MFLCode = LTFR.MFLCode GROUP BY LHTSE.HIVRiskCategory, LHTSE.County ) AS Subquery GROUP BY County;

                    when asked for a specific period, please add the date filter

                     Example 1:
                    Question: What is the percentage of individuals with negative final HIV test results in December 2023 who were enrolled on PrEP, categorized by HIV risk category (Very High, High, Moderate), and grouped by county?
                    SQLQuery: SELECT LTFR.County, SUM(CASE WHEN LHTSE.HIVRiskCategory = 'Very High' AND LTFR.FinalTestResult = 'Negative' THEN 1 ELSE 0 END) AS VeryHigh_Total, SUM(CASE WHEN LHTSE.HIVRiskCategory = 'High' AND LTFR.FinalTestResult = 'Negative' THEN 1 ELSE 0 END) AS High_Total, SUM(CASE WHEN LHTSE.HIVRiskCategory = 'Moderate' AND LTFR.FinalTestResult = 'Negative' THEN 1 ELSE 0 END) AS Moderate_Total, SUM(CASE WHEN LHTSE.HIVRiskCategory = 'Very High' AND LTFR.FinalTestResult = 'Negative' AND LPA.PrepEnrollmentDate IS NOT NULL THEN 1 ELSE 0 END) AS VeryHigh_Negative_EnrolledOnPrep, SUM(CASE WHEN LHTSE.HIVRiskCategory = 'High' AND LTFR.FinalTestResult = 'Negative' AND LPA.PrepEnrollmentDate IS NOT NULL THEN 1 ELSE 0 END) AS High_Negative_EnrolledOnPrep, SUM(CASE WHEN LHTSE.HIVRiskCategory = 'Moderate' AND LTFR.FinalTestResult = 'Negative' AND LPA.PrepEnrollmentDate IS NOT NULL THEN 1 ELSE 0 END) AS Moderate_Negative_EnrolledOnPrep, (SUM(CASE WHEN LHTSE.HIVRiskCategory = 'Very High' AND LTFR.FinalTestResult = 'Negative' AND LPA.PrepEnrollmentDate IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN LHTSE.HIVRiskCategory = 'Very High' AND LTFR.FinalTestResult = 'Negative' THEN 1 ELSE 0 END), 0)) AS VeryHigh_Percentage_Negative_EnrolledOnPrep, (SUM(CASE WHEN LHTSE.HIVRiskCategory = 'High' AND LTFR.FinalTestResult = 'Negative' AND LPA.PrepEnrollmentDate IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN LHTSE.HIVRiskCategory = 'High' AND LTFR.FinalTestResult = 'Negative' THEN 1 ELSE 0 END), 0)) AS High_Percentage_Negative_EnrolledOnPrep, (SUM(CASE WHEN LHTSE.HIVRiskCategory = 'Moderate' AND LTFR.FinalTestResult = 'Negative' AND LPA.PrepEnrollmentDate IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN LHTSE.HIVRiskCategory = 'Moderate' AND LTFR.FinalTestResult = 'Negative' THEN 1 ELSE 0 END), 0)) AS Moderate_Percentage_Negative_EnrolledOnPrep FROM (SELECT DISTINCT PatientPKHash, MFLCode, HIVRiskCategory FROM LinelistHTSEligibilty) AS LHTSE JOIN (SELECT DISTINCT PatientPKHash, MFLCode, FinalTestResult, County FROM LineListTransHTS WHERE TestDate >= '2023-10-01' AND TestDate < '2024-01-01') AS LTFR ON LHTSE.PatientPKHash = LTFR.PatientPKHash AND LHTSE.MFLCode = LTFR.MFLCode LEFT JOIN (SELECT DISTINCT PatientPKHash, MFLCode, PrepEnrollmentDate FROM LinelistPrepAssessments WHERE PrepEnrollmentDate >='2023-10-01') AS LPA ON LHTSE.PatientPKHash = LPA.PatientPKHash AND LHTSE.MFLCode = LPA.MFLCode GROUP BY LTFR.County;

                    Adjust the date filtering and grouping variable according to the question asked, for example, if the question is about November 2023, change the TestDate and Prep enrollment date to November 2023, and similarly,if the question is group by Partner Change from County to PartnerName. Prep enrollment date should always be greater than or equal to test date
                    Only use tables listed in the schema .

                """

        response_list, metadata_dict = nl_sql_retriever.retrieve_with_metadata(
            custom_txt2sql_prompt + nl_query.question)

        sql_query = metadata_dict["sql_query"]
        log.debug(f"Generated SQL query: {sql_query}")

        with SessionLocal() as session:
            result = session.execute(text(sql_query))
            rows = result.fetchall()
            # Get column names
            columns = result.keys()

            data = [dict(zip(columns, row)) for row in rows]

        return {"sql_query": sql_query, "data": data}
    except Exception as e:
        log.error(f"Error processing query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Endpoint to retrieve table descriptions
@router.get('/table_descriptions')
async def get_table_descriptions():
    try:
        descriptions = []
        tables_info = get_dictionary_info_cached()
        for table in tables_info:
            descriptions.append({
                "table_name": table.table_name,
                "description": table.context_str
            })
        return {"tables": descriptions}
    except Exception as e:
        log.error(f"Error retrieving table descriptions: {e}")
        raise HTTPException(status_code=500, detail=str(e))
