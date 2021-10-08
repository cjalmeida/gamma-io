from typing import Tuple
from pydantic import BaseModel


class ConfigType(BaseModel):
    __discriminator__ = "kind"

    @classmethod
    def get_subclasses(cls):
        stack = [cls]
        while stack:
            cc = stack.pop(0)
            for sub in cc.__subclasses__():
                stack.append(sub)
                yield sub

    @classmethod
    def parse_string(cls, value: str):
        kind = value
        args = {cls.__discriminator__: value}
        return kind, args

    @classmethod
    def __get_validators__(cls):
        def subtype_kind(t):
            disc = t.__discriminator__
            try:
                kind = getattr(t, disc)
                return kind
            except AttributeError:
                pass

            try:
                _field = t.__fields__[disc]
                kind = getattr(_field, "default", None)
                if kind is None:
                    raise ValueError(f"Field {t.__name__}.{disc} has no value set")
                return kind
            except KeyError:
                pass

            raise ValueError(f"Type '{t.__name__}' missing field '{disc}' ")

        def validator(value):
            if isinstance(value, str):
                ret = cls.parse_string(value)
                if isinstance(ret, Tuple):
                    kind, args = ret
                else:
                    return ret
            else:
                try:
                    kind = value[cls.__discriminator__]
                    args = value
                except KeyError:
                    raise ValueError(f"Missing '{cls.__discriminator__}' field.")
            match = [k for k in cls.get_subclasses() if subtype_kind(k) == kind]
            if not match:
                raise ValueError(f"No {cls.__name__} for kind '{kind}'")
            return match[0](**args)

        yield validator
