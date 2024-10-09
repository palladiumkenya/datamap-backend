from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from cassandra.cqlengine.management import sync_table
from routes import access_api, dictionary_mapper_api, data_dictionary_api, text2sql_api
from models.models import AccessCredentials, MappedVariables, DataDictionaries, DataDictionaryTerms, DataDictionariesUSL, DataDictionaryTermsUSL

app = FastAPI()

origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    sync_table(AccessCredentials)
    sync_table(MappedVariables)
    sync_table(DataDictionaries)
    sync_table(DataDictionaryTerms)


app.include_router(access_api.router, tags=['Access'], prefix='/api/db_access')
app.include_router(dictionary_mapper_api.router, tags=['Selector'], prefix='/api/dictionary_mapper')
app.include_router(data_dictionary_api.router, tags=['Data Dictionary'], prefix='/api/data_dictionary')
# app.include_router(text2sql_api.router, tags=['Text2SQL'], prefix='/api/text2sql')


@app.get("/api/healthchecker")
def root():
    return {"message": "Welcome to data map, we are up and running"}


# Run the FastAPI application
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
