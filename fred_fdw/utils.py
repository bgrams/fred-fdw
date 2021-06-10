import logging

import jq
from fredio.utils import AbstractQueryEngine
from multicorn.utils import log_to_postgres


class PgHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        log_to_postgres(self.format(record), record.levelno)


class JQEngine(AbstractQueryEngine):
    def _compile(self, query):
        return jq.compile(query)

    def _execute(self, data):
        return self._compiled.input(data)


engine = JQEngine()
