import yaml
from pathlib import Path
from typing import Any, Dict


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration settings from a YAML file."""
    with Path(config_path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)
