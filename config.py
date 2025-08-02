from pydantic_settings import BaseSettings



class Settings(BaseSettings):

    db_name: str 
    db_host: str 
    db_username: str 
    db_password: str 
    db_port: int 
    admin_username: str 
    admin_password: str

    class Config:
        env_file = ".env"


settings = Settings()
