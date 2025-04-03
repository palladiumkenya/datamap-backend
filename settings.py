from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB: str
    DB_USER: str
    DB_PASSWORD: str

    STAGING_API: str
    BATCH_SIZE: int
    JWT_SECRET_KEY: str
    REFRESH_SECRET_KEY: str
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
