from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class DBConfig:
    DBMS: str
    DRIVER: str
    HOSTNAME: str
    DATABASE: str
    USERNAME: str
    PASSWORD: str
    config_name: str


class DBConfigInstance:

    def __init__(self, in_db_config: DBConfig):
        self._DBMS = in_db_config.DBMS
        self._DRIVER = in_db_config.DRIVER
        self._HOSTNAME = in_db_config.HOSTNAME
        self._DATABASE = in_db_config.DATABASE
        self._USERNAME = in_db_config.USERNAME
        self._PASSWORD = in_db_config.PASSWORD
        self.config_name = in_db_config.config_name
        self.DB_URI = self._set_db_url()

    def _set_db_url(self):
        db_url = '{}+{}://{}:{}@{}/{}'.format(
            self._DBMS, self._DRIVER, self._USERNAME,
            self._PASSWORD, self._HOSTNAME, self._DATABASE
        )
        return db_url

    def __repr__(self):
        print_result = (f"{self.config_name}("
                        f"DBMS='{self._DBMS}', "
                        f"DRIVER='{self._DRIVER}', "
                        f"USERNAME='{self._USERNAME}', "
                        f"PASSWORD='{self._PASSWORD}', "
                        f"HOSTNAME='{self._HOSTNAME}', "
                        f"DATABASE='{self._DATABASE}'"
                        ")")
        return print_result


_ANALYTICS_BASE_DB_CONFIG = DBConfig(DBMS='postgresql',
                                     DRIVER='psycopg2',
                                     HOSTNAME='localhost',
                                     DATABASE='analytics_base',
                                     USERNAME='postgres',
                                     PASSWORD='xxXX1234',
                                     config_name='ANALYTICS_BASE_DB_CONFIG')

ANALYTICS_BASE_DB_CONFIG = DBConfigInstance(_ANALYTICS_BASE_DB_CONFIG)
