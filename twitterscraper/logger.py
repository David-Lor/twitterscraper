import sys

from loguru import logger
from .settings import LogSettings

__all__ = ["logger", "setup_loggers"]

DefaultLoggerFormat = "<green>{time:YY-MM-DD HH:mm:ss}</green> | " \
               "<level>{level}</level> | " \
               "{function}: <level>{message}</level> | " \
               "{extra} {exception}"


def setup_loggers(settings: LogSettings):
    logger.remove()
    if settings.file.enabled:
        logger.add(
            sink=settings.file.path,
            level=settings.file.level.upper(),
            serialize=True,
        )
    if settings.stdout.enabled:
        logger.add(
            sink=sys.stdout,
            level=settings.stdout.level.upper(),
            format=settings.stdout.format or DefaultLoggerFormat,
        )
