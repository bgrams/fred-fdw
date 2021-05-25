import abc
import asyncio
import atexit
import inspect
import itertools
import logging
import os
from typing import (Any,
                    Callable,
                    Dict,
                    Generator,
                    List,
                    Mapping,
                    Optional,
                    Set,
                    Tuple,
                    Union)

import fredio
from multicorn import ForeignDataWrapper, Qual, SortKey, TableDefinition, ColumnDefinition
from multicorn.utils import log_to_postgres


loop = asyncio.get_event_loop()


class PgHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        log_to_postgres(self.format(record), record.levelno)


class Column(ColumnDefinition):

    def __init__(self,
                 *args,
                 allowed: Optional[List[Union[str, Tuple]]] = None,
                 alias: Optional[str] = None,
                 cleaner: Optional[Callable[[Any], Any]] = None,
                 default: Optional[Any] = None,
                 parameter: bool = False,
                 resolvers: Optional[Dict[str, Callable[[Qual], Tuple[str, Any]]]] = None,
                 required: bool = False,
                 **kwargs):

        super().__init__(*args, **kwargs)

        if default and (required or not parameter):
            raise ValueError("Default values can only be used for non-required parameters")

        self.allowed = allowed
        self.alias = alias or self.column_name
        self.cleaner = cleaner
        self.default = default
        self.required = required
        self.resolvers = resolvers or {}
        self.parameter = parameter

    def resolve(self, qual: Qual) -> Tuple[str, Any]:
        if self.allowed is not None:
            errmsg = "Operator %s not supported for %s"
            assert qual.operator in self.allowed, errmsg % (qual.operator, self.column_name)

        resolver = self.resolvers.get(qual.operator, lambda x: (self.alias, x.value))
        return resolver(qual)


class MetaTable(abc.ABCMeta):

    registry: Dict[str, "MetaTable"] = {}
    definition: TableDefinition

    def __new__(mcs, name, bases, dct):
        columns = dict(filter(lambda x: isinstance(x[1], Column), dct.items()))

        dct["columns"] = columns
        dct.setdefault("__table_name__", name.lower())
        dct.setdefault("__table_args__", {})

        # Set these upfront bc frequent access
        dct["cleaners"] = {}
        dct["defaults"] = {}
        dct["required"] = set()
        dct["parameters"] = set()

        for col in columns.values():
            if col.cleaner is not None:
                dct["cleaners"][col.alias] = col.cleaner
            if col.default is not None:
                dct["defaults"][col.alias] = col.default
            if col.required:
                dct["required"].add(col.alias)
            if col.parameter:
                dct["parameters"].add(col.column_name)

        dct["definition"] = TableDefinition(
            dct["__table_name__"],
            columns=columns.values(),
            options={"table": name})

        klass = super().__new__(mcs, name, bases, dct)

        if not inspect.isabstract(klass):
            mcs.registry[name] = klass  # type: ignore

        return klass


class ForeignTable(ForeignDataWrapper, metaclass=MetaTable):

    __table_name__: str
    __table_args__: Mapping[str, Any]

    columns: Dict[str, Column]

    cleaners: Dict[str, Callable]
    defaults: Dict[str, Any]
    required: Set[str]
    parameters: Set[str]

    clients: Dict[str, fredio.client.ApiClient] = {}

    def __new__(cls, fdw_options, fdw_columns) -> "ForeignTable":
        table = fdw_options.pop("table", None)
        klass = MetaTable.registry.get(table)
        return super().__new__(klass)

    def __init__(self, fdw_options, fdw_columns) -> None:

        super().__init__(fdw_options, fdw_columns)

        # Can still reference env FRED_API_KEY
        # But recommended to use user mapping options
        api_key = fdw_options.get("api_key", None)

        self.client = self.get_or_create_client(api_key)
        self.options = fdw_options

        self.logger = self.setup_logger(fdw_options)
        self.logger.debug("PID %d" % os.getpid())

    @property
    @abc.abstractmethod
    def endpoint(self) -> fredio.client.Endpoint: ...

    @classmethod
    def get_or_create_client(cls, api_key) -> fredio.client.ApiClient:
        return cls.clients.setdefault(
            api_key, fredio.configure(api_key=api_key)
        )

    @classmethod
    def close_all_clients(cls) -> None:
        for client in cls.clients.values():
            client.close()

    @classmethod
    def import_schema(cls, *args, **kwargs) -> List[TableDefinition]:
        return [t.definition for t in MetaTable.registry.values()]

    def resolve(self, quals: List[Qual]) -> List[Dict[str, str]]:
        """
        Map qualifiers to API parameters
        """
        params = {
            k: v if isinstance(v, (list, set, tuple)) else [v]
            for k, v in self.defaults.items()
        }

        for qual in filter(lambda x: x.field_name in self.parameters, quals):

            name, value = self.columns[qual.field_name].resolve(qual)

            if isinstance(value, (list, set, tuple)):
                value = list(map(str, value))
                params.setdefault(name, []).extend(value)
            else:
                params.setdefault(name, []).append(str(value))

        if self.required:
            reqdiff = self.required.difference(params.keys())
            assert not reqdiff, "%s predicates are required" % ", ".join(self.required)

        values = itertools.product(*list(params.values()))
        return list(map(lambda x: dict(zip(params.keys(), x)), values))

    def setup_logger(self, options: Mapping[str, Any]) -> logging.Logger:

        lvl = options.get("log_level", logging.NOTSET)
        fmt = options.get("log_format", logging.BASIC_FORMAT)

        handler = PgHandler()
        handler.setFormatter(logging.Formatter(fmt))

        logger = logging.getLogger(self.__table_name__)
        logger.addHandler(handler)
        logger.setLevel(logging.getLevelName(lvl))

        # Only the first call will have an effect
        logging.basicConfig(level=lvl, format=fmt)

        return logger

    def execute(self,
                quals: List[Qual],
                columns: Set[str],
                sortkeys: Optional[List[SortKey]] = None
                ) -> Generator[Dict[str, Any], None, None]:

        param_list = self.resolve(quals)
        param_coro = list(map(
            lambda x: self.endpoint.aget(**x, jsonpath=self.__table_args__.get("jsonpath")),
            param_list
        ))

        results = loop.run_until_complete(asyncio.gather(*param_coro))

        for param, result in zip(param_list, results):
            for element in result:
                param.update(element)
                param.update({k: clean(param[k]) for k, clean in self.cleaners.items()})
                yield param


atexit.register(ForeignTable.close_all_clients)


class Observation(ForeignTable):

    __table_name__ = "series_observation"
    __table_args__ = {
        "jsonpath": "observations[*]",
    }

    series_id = Column("series_id", type_name="text", required=True, allowed=["=", ("=", True)], parameter=True)
    realtime_start = Column("realtime_start", type_name="date", allowed=["="], parameter=True)
    realtime_end = Column("realtime_end", type_name="date", allowed=["="], parameter=True)
    date = Column(
        "date",
        type_name="date",
        parameter=True,
        resolvers={
            ">=": lambda x: ("observation_start", x.value),
            "<=": lambda x: ("observation_end", x.value),
        })
    value = Column("value", type_name="numeric", cleaner=lambda x: None if x == "." else x)
    units = Column("units", type_name="text", default="lin", parameter=True)
    output_type = Column("output_type", type_name="smallint", default=1, parameter=True)

    # Share these resolvers bc it's only like 2 extra rows being requested
    date.resolvers[">"] = date.resolvers[">="]
    date.resolvers["<"] = date.resolvers["<="]

    @property
    def endpoint(self):
        return self.client.series.observations


class Series(ForeignTable):

    __table_name__ = "series"
    __table_args__ = {
        "jsonpath": "seriess[*]",
    }

    id = Column(
        "id", type_name="text", required=True, allowed=["=", ("=", True)],
        alias="series_id", parameter=True
    )
    realtime_start = Column("realtime_start", type_name="date", allowed=["="], parameter=True)
    realtime_end = Column("realtime_end", type_name="date", allowed=["="], parameter=True)
    title = Column("title", type_name="text")
    observation_start = Column("observation_start", type_name="date")
    observation_end = Column("observation_end", type_name="date")
    frequency = Column("frequency", type_name="text")
    frequency_short = Column("frequency_short", type_name="varchar(10)")
    units = Column("units", type_name="text")
    units_short = Column("units_short", type_name="text")
    seasonal_adjustment = Column("seasonal_adjustment", type_name="text")
    seasonal_adjustment_short = Column("seasonal_adjustment_short", type_name="varchar(10)")
    last_updated = Column("last_updated", type_name="timestamp with time zone")
    popularity = Column("popularity", type_name="numeric")
    notes = Column("notes", type_name="text")

    @property
    def endpoint(self):
        return self.client.series
