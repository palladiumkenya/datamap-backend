from pydantic import BaseSettings


class Settings(BaseSettings):
    CASSANDRA_HOST: str
    CASSANDRA_PORT: int
    CASSANDRA_DB: str
    CASSANDRA_USER: str
    CASSANDRA_PASSWORD: str

    class Config:
        env_file = '.env'


settings = Settings()
