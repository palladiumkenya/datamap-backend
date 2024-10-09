from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    CASSANDRA_HOST: str
    CASSANDRA_PORT: int
    CASSANDRA_DB: str
    CASSANDRA_USER: str
    CASSANDRA_PASSWORD: str

    # REPORTING_DB: str
    # REPORTING_USER: str
    # REPORTING_PASSWORD: str
    # REPORTING_HOST: str
    # OPENAI_KEY: str

    # OM_HOST: str
    # OM_JWT: str

    class Config:
        env_file = './.env'
        extra = 'ignore'


settings = Settings()
