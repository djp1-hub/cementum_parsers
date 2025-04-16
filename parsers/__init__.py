import importlib
import os
import pkgutil
from typing import Dict, Type

from .base import BaseParser

PARSERS: Dict[str, Type[BaseParser]] = {}

# Автоматически подгружаем все модули в папке
package_dir = os.path.dirname(__file__)
for _, module_name, _ in pkgutil.iter_modules([package_dir]):
    if module_name not in {"__init__", "base"}:
        module = importlib.import_module(f"{__name__}.{module_name}")
        for attr in dir(module):
            obj = getattr(module, attr)
            if isinstance(obj, type) and issubclass(obj, BaseParser) and obj is not BaseParser:
                PARSERS[obj.name] = obj
