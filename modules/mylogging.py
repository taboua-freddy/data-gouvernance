import logging

class MyLogger:

    def __init__(self, name: str, with_console: bool = True, with_file: bool = True):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # Gestionnaire pour l'affichage console
        if with_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        # Gestionnaire pour l'enregistrement des logs dans un fichier
        if with_file:
            info_handler = logging.FileHandler(f'{name}_info.log')
            info_handler.setLevel(logging.INFO)
            info_handler.setFormatter(formatter)
            self.logger.addHandler(info_handler)
            error_handler = logging.FileHandler(f'{name}_error.log')
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(formatter)
            self.logger.addHandler(error_handler)

    def info(self, message: str):
        self.logger.info(message)

    def error(self, message: str):
        self.logger.error(message)

    def warning(self, message: str):
        self.logger.warning(message)

    def debug(self, message: str):
        self.logger.debug(message)