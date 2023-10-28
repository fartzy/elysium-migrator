import logging
import logging.handlers
import os


class Logger:
    handler = None
    log_path = None
    
    def __init__(self, log_level="DEBUG", log_path="app.log"):
        self.log = logging.getLogger(__name__)
        self.log.setLevel(os.environ.get("LOGLEVEL", log_level))
        
        if not self.__class__.get_handler():
        handler = logging.handlers.WatchedFileHandler(log_path, mode="w")
        self.__class__.set_handler(handler)
        logging.basicConfig(
            format="%(asctime)s %(name)-12s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=[self.__class__.get_handler()],
        )

    @classmethod
    def set_handler(cls, handler):
        if cls.handler is None:
            cls.handler = handler
            
    @classmethod
    def get_handler(cls):
        return cls.handler
    
    @classmethod
    def set_log_path(cls, log_path):
        if cls.log_path is None:
            cls.log_path = log_path
            
    @classmethod
    def get_log_path(cls):
        return cls.log_path   
        
    def set(self, log_level, log_path):
        self.log.setLevel(os.environ.get("LOGLEVEL", log_level))
        handler = logging.FileHandler(
            os.environ.get("VALHALLA_MIGRATE_LOGPATH", log_path), mode="w"
        )
        logging.basicConfig(
            format="%(asctime)s %(name)-12s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=self.log.handlers + [handler],
        )

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
