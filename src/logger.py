from datetime import datetime
from enum import Enum

from colorama import Back, Fore, Style


class LogLevel(Enum):

  FATAL = 0
  ERROR = 2
  WARN  = 4
  INFO  = 6
  DEBUG = 8
  TRACE = 10

  @staticmethod
  def from_string(log_level: str):
    if log_level == 'TRACE':
      return LogLevel.TRACE
    elif log_level == 'DEBUG':
      return LogLevel.DEBUG
    elif log_level == 'INFO':
      return LogLevel.INFO
    elif log_level == 'WARN':
      return LogLevel.WARN
    elif log_level == 'ERROR':
      return LogLevel.ERROR
    elif log_level == 'FATAL':
      return LogLevel.FATAL
    
    return LogLevel.INFO


class Logger(object):

  context_key: str = None
  log_level: LogLevel = None

  def __init__(self, context_key: str, log_level: LogLevel = LogLevel.INFO):
    self.context_key = context_key
    self.log_level = log_level

  def log(self, log_level: LogLevel, message: str):
    if self.log_level.value < log_level.value:
      return

    timestamp = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'
  
    # e.g. [INFO - SUPPORT] @ [2020-09-09T06:34:01.815Z] : some text goes here 
    text_to_print = '[%s - %s] @ [%s] : %s' % (
        log_level.name,
        self.context_key,
        timestamp,
        message
      )
    foreground_colour = Fore.WHITE
    background_colour = ''

    if log_level == LogLevel.FATAL:
      foreground_colour = f'{Style.BRIGHT}{Fore.YELLOW}'
      background_colour = Back.RED
    elif log_level == LogLevel.ERROR:
      foreground_colour = f'{Style.BRIGHT}{Fore.RED}'
    elif log_level == LogLevel.WARN:
      foreground_colour = f'{Style.BRIGHT}{Fore.YELLOW}'
    elif log_level == LogLevel.DEBUG:
      foreground_colour = f'{Style.BRIGHT}{Fore.GREEN}'
    elif log_level == LogLevel.TRACE:
      foreground_colour = f'{Style.BRIGHT}{Fore.MAGENTA}'

    print(f'{background_colour}{foreground_colour}{text_to_print}{Style.RESET_ALL}')


class ContextLogger(object):

  __instance: 'ContextLogger' = None
  __sys_logger: Logger = Logger('SYSTEM', LogLevel.TRACE)
  context_logger_map: dict = {}
  
  def __init__(self):
    self.context_logger_map = {}
    self.context_logger_map['SYSTEM'] = ContextLogger.__sys_logger

  @staticmethod
  def instance():
    return ContextLogger.__instance if ContextLogger.__instance is not None else ContextLogger.initialize()

  @staticmethod
  def initialize():
    if ContextLogger.__instance is not None:
      return ContextLogger.__instance
      
    ContextLogger.__sys_logger.log(LogLevel.INFO, '[ContextLogger] initializing...')

    ContextLogger.__instance = ContextLogger()

    return ContextLogger.__instance

  def create_logger_for_context(self, context_key: str, log_level: LogLevel):
    if context_key in self.context_logger_map:
      ContextLogger.sys_log(LogLevel.WARN, 'logger already exists for [%s]. Not recreating.' % context_key)
      return

    self.context_logger_map[context_key] = Logger(context_key, log_level)

  def set_log_level(self, context_key: str, log_level: LogLevel):
    if context_key not in ContextLogger.instance().context_logger_map:
      ContextLogger.sys_log(LogLevel.WARN, 'no logger defined for [%s].' % context_key)
      return

    if type(log_level) == 'str':
      log_level = LogLevel.from_string(log_level)

    ContextLogger.instance().context_logger_map[context_key].log_level = log_level


  @staticmethod
  def sys_log(log_level: LogLevel, message: str):
    logger = ContextLogger.instance().context_logger_map['SYSTEM']
    logger.log(log_level, message)

  @staticmethod
  def log(context_key: str, log_level: LogLevel, message: str):
    logger: Logger = None

    if context_key not in ContextLogger.instance().context_logger_map:
      ContextLogger.sys_log(LogLevel.WARN, 'no logger defined for [%s].' % context_key)
      return

    logger = ContextLogger.instance().context_logger_map[context_key]
    logger.log(log_level, message)

  @staticmethod
  def info(context_key: str, message: str):
    logger: Logger = None

    if context_key not in ContextLogger.instance().context_logger_map:
      ContextLogger.sys_log(LogLevel.WARN, 'no logger defined for [%s].' % context_key)
      return

    logger = ContextLogger.instance().context_logger_map[context_key]
    logger.log(LogLevel.INFO, message)

  @staticmethod
  def debug(context_key: str, message: str):
    logger: Logger = None

    if context_key not in ContextLogger.instance().context_logger_map:
      ContextLogger.sys_log(LogLevel.WARN, 'no logger defined for [%s].' % context_key)
      return

    logger = ContextLogger.instance().context_logger_map[context_key]
    logger.log(LogLevel.DEBUG, message)

  @staticmethod
  def warn(context_key: str, message: str):
    logger: Logger = None

    if context_key not in ContextLogger.instance().context_logger_map:
      ContextLogger.sys_log(LogLevel.WARN, 'no logger defined for [%s].' % context_key)
      return

    logger = ContextLogger.instance().context_logger_map[context_key]
    logger.log(LogLevel.WARN, message)

  @staticmethod
  def error(context_key: str, message: str):
    logger: Logger = None

    if context_key not in ContextLogger.instance().context_logger_map:
      ContextLogger.sys_log(LogLevel.WARN, 'no logger defined for [%s].' % context_key)
      return

    logger = ContextLogger.instance().context_logger_map[context_key]
    logger.log(LogLevel.ERROR, message)

  @staticmethod
  def trace(context_key: str, message: str):
    logger: Logger = None

    if context_key not in ContextLogger.instance().context_logger_map:
      ContextLogger.sys_log(LogLevel.WARN, 'no logger defined for [%s].' % context_key)
      return

    logger = ContextLogger.instance().context_logger_map[context_key]
    logger.log(LogLevel.TRACE, message)
