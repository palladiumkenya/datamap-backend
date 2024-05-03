from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from cassandra.cqlengine.management import sync_table
from routes import access_api,indicator_selector_api
from models.models import AccessCredentials, IndicatorVariables

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
    sync_table(IndicatorVariables)


app.include_router(access_api.router, tags=['Access'], prefix='/api/db_access')
app.include_router(indicator_selector_api.router, tags=['Selector'], prefix='/api/indicator_selector')


@app.get("/api/healthchecker")
def root():
    return {"message": "Welcome to data map, we are up and running"}


# Run the FastAPI application
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
