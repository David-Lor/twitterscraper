import asyncio
import datetime
import dataclasses
from collections import defaultdict
from enum import Enum
from pathlib import PurePath
from types import GeneratorType
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import pydantic
import pydantic.json
import dateutil.relativedelta
import dateutil.rrule


class DateMonth(pydantic.BaseModel):
    year: int
    month: int

    @property
    def date_start(self):
        return datetime.date(self.year, self.month, 1)

    @property
    def date_end_exclusive(self):
        return self.date_start + dateutil.relativedelta.relativedelta(months=1)

    @property
    def date_end_inclusive(self):
        return self.date_end_exclusive - datetime.timedelta(days=1)

    @property
    def isoformat(self):
        return f"{self.year}-{self.month:02d}"

    @classmethod
    def from_isoformat(cls, s: str):
        chunks = s.split("-")
        return cls(year=chunks[0], month=chunks[1])

    @classmethod
    def from_date(cls, d: datetime.date):
        return cls.from_isoformat(d.isoformat())


def daterange_by_month(start_date: datetime.date, end_date_inc: datetime.date | None = None):
    """Iterate each month between two dates, yielding each found month, including the month of `end_date_inc`.
    If `end_date_inc` is None, use current date.
    """
    if end_date_inc is None:
        end_date_inc = datetime.date.today()

    for date_iter in dateutil.rrule.rrule(dateutil.rrule.MONTHLY, dtstart=start_date, until=end_date_inc):
        yield DateMonth.from_date(date_iter)


async def async_gather_limited(n, *coros):
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro

    return await asyncio.gather(*(sem_coro(c) for c in coros))


def generate_encoders_by_class_tuples(
        type_encoder_map: Dict[Any, Callable[[Any], Any]]
) -> Dict[Callable[[Any], Any], Tuple[Any, ...]]:
    _encoders_by_class_tuples: Dict[Callable[[Any], Any], Tuple[Any, ...]] = defaultdict(
        tuple
    )
    for type_, encoder in type_encoder_map.items():
        _encoders_by_class_tuples[encoder] += (type_,)
    return _encoders_by_class_tuples


encoders_by_class_tuples = generate_encoders_by_class_tuples(pydantic.json.ENCODERS_BY_TYPE)
SetIntStr = Set[Union[int, str]]
DictIntStrAny = Dict[Union[int, str], Any]


def jsonable_encoder(
        obj: Any,
        include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
        exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
        by_alias: bool = True,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        custom_encoder: Optional[Dict[Any, Callable[[Any], Any]]] = None,
        sqlalchemy_safe: bool = True,
) -> Any:
    # SOURCE: https://github.com/tiangolo/fastapi/blob/7d865c9487eb8d7e6bcb508b933b17a5a6e8586b/fastapi/encoders.py#L29
    custom_encoder = custom_encoder or {}
    if custom_encoder:
        if type(obj) in custom_encoder:
            return custom_encoder[type(obj)](obj)
        else:
            for encoder_type, encoder_instance in custom_encoder.items():
                if isinstance(obj, encoder_type):
                    return encoder_instance(obj)
    if include is not None and not isinstance(include, (set, dict)):
        include = set(include)
    if exclude is not None and not isinstance(exclude, (set, dict)):
        exclude = set(exclude)
    if isinstance(obj, pydantic.BaseModel):
        encoder = getattr(obj.__config__, "json_encoders", {})
        if custom_encoder:
            encoder.update(custom_encoder)
        obj_dict = obj.dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_none=exclude_none,
            exclude_defaults=exclude_defaults,
        )
        if "__root__" in obj_dict:
            obj_dict = obj_dict["__root__"]
        return jsonable_encoder(
            obj_dict,
            exclude_none=exclude_none,
            exclude_defaults=exclude_defaults,
            custom_encoder=encoder,
            sqlalchemy_safe=sqlalchemy_safe,
        )
    if dataclasses.is_dataclass(obj):
        obj_dict = dataclasses.asdict(obj)
        return jsonable_encoder(
            obj_dict,
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            custom_encoder=custom_encoder,
            sqlalchemy_safe=sqlalchemy_safe,
        )
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, PurePath):
        return str(obj)
    if isinstance(obj, (str, int, float, type(None))):
        return obj
    if isinstance(obj, dict):
        encoded_dict = {}
        allowed_keys = set(obj.keys())
        if include is not None:
            allowed_keys &= set(include)
        if exclude is not None:
            allowed_keys -= set(exclude)
        for key, value in obj.items():
            if (
                    (
                            not sqlalchemy_safe
                            or (not isinstance(key, str))
                            or (not key.startswith("_sa"))
                    )
                    and (value is not None or not exclude_none)
                    and key in allowed_keys
            ):
                encoded_key = jsonable_encoder(
                    key,
                    by_alias=by_alias,
                    exclude_unset=exclude_unset,
                    exclude_none=exclude_none,
                    custom_encoder=custom_encoder,
                    sqlalchemy_safe=sqlalchemy_safe,
                )
                encoded_value = jsonable_encoder(
                    value,
                    by_alias=by_alias,
                    exclude_unset=exclude_unset,
                    exclude_none=exclude_none,
                    custom_encoder=custom_encoder,
                    sqlalchemy_safe=sqlalchemy_safe,
                )
                encoded_dict[encoded_key] = encoded_value
        return encoded_dict
    if isinstance(obj, (list, set, frozenset, GeneratorType, tuple)):
        encoded_list = []
        for item in obj:
            encoded_list.append(
                jsonable_encoder(
                    item,
                    include=include,
                    exclude=exclude,
                    by_alias=by_alias,
                    exclude_unset=exclude_unset,
                    exclude_defaults=exclude_defaults,
                    exclude_none=exclude_none,
                    custom_encoder=custom_encoder,
                    sqlalchemy_safe=sqlalchemy_safe,
                )
            )
        return encoded_list

    if type(obj) in pydantic.json.ENCODERS_BY_TYPE:
        return pydantic.json.ENCODERS_BY_TYPE[type(obj)](obj)
    for encoder, classes_tuple in encoders_by_class_tuples.items():
        if isinstance(obj, classes_tuple):
            return encoder(obj)

    try:
        data = dict(obj)
    except Exception as e:
        errors: List[Exception] = [e]
        try:
            data = vars(obj)
        except Exception as e:
            errors.append(e)
            raise ValueError(errors) from e
    return jsonable_encoder(
        data,
        include=include,
        exclude=exclude,
        by_alias=by_alias,
        exclude_unset=exclude_unset,
        exclude_defaults=exclude_defaults,
        exclude_none=exclude_none,
        custom_encoder=custom_encoder,
        sqlalchemy_safe=sqlalchemy_safe,
    )
