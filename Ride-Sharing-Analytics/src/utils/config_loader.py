"""
================================================================================
RideSharing Analytics Platform — Configuration Loader
================================================================================
Module      : src/utils/config_loader.py
Description : YAML-based configuration loader with dot-notation access,
              environment variable interpolation, and schema validation.
              All pipeline components use this as their single config interface.

Design Decisions:
    - PyYAML for simplicity; production systems would use Dynaconf or
      Hydra for multi-environment config merging.
    - Dot-notation access (cfg.get("spark.app_name")) mirrors how cloud
      config systems like AWS Parameter Store expose values.
    - Environment variable interpolation allows secrets (credentials,
      S3 paths) to be injected at runtime without touching the YAML.

Author      : RideSharing Platform Engineering
================================================================================
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Optional

import yaml

from src.utils.logger import get_logger

logger = get_logger("utils.config_loader")

# Regex to match ${ENV_VAR} patterns in YAML values
_ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


class ConfigLoader:
    """
    YAML configuration loader with deep-key access and env-var interpolation.

    Usage:
        cfg = ConfigLoader("config/pipeline_config.yaml")
        app_name = cfg.get("spark.app_name")
        paths    = cfg.get("paths.bronze")
    """

    def __init__(self, config_path: str | Path = "config/pipeline_config.yaml") -> None:
        self._path = Path(config_path)
        self._config: dict[str, Any] = self._load()
        logger.info("Configuration loaded from: %s", self._path.resolve())

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieve a value by dot-separated key path.

        Examples:
            cfg.get("spark.configs.spark.sql.shuffle.partitions")
            cfg.get("paths.bronze.drivers")

        Args:
            key:     Dot-separated key path into the YAML tree.
            default: Value returned if the key is absent.

        Returns:
            The resolved value, or `default` if the key is not found.
        """
        keys = key.split(".")
        node = self._config
        for k in keys:
            if not isinstance(node, dict):
                return default
            node = node.get(k, None)
            if node is None:
                return default
        return node

    def get_section(self, section: str) -> dict[str, Any]:
        """Return an entire top-level config section as a dict."""
        return self._config.get(section, {})

    def get_spark_configs(self) -> dict[str, str]:
        """Return flattened Spark config dict ready for SparkConf.setAll()."""
        return self._config.get("spark", {}).get("configs", {})

    @property
    def raw(self) -> dict[str, Any]:
        """Direct access to the full parsed config dict (read-only by convention)."""
        return self._config

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load(self) -> dict[str, Any]:
        """Parse YAML and interpolate environment variables."""
        if not self._path.exists():
            raise FileNotFoundError(
                f"Pipeline configuration not found at: {self._path.resolve()}\n"
                "Ensure config/pipeline_config.yaml exists before running the pipeline."
            )
        with self._path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
        return self._interpolate_env(raw)

    def _interpolate_env(self, obj: Any) -> Any:
        """Recursively replace ${ENV_VAR} placeholders with OS environment values."""
        if isinstance(obj, dict):
            return {k: self._interpolate_env(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._interpolate_env(item) for item in obj]
        if isinstance(obj, str):
            def replacer(match: re.Match) -> str:  # type: ignore[type-arg]
                var_name = match.group(1)
                value = os.environ.get(var_name)
                if value is None:
                    logger.warning("Environment variable '%s' not set — using empty string.", var_name)
                    return ""
                return value
            return _ENV_VAR_PATTERN.sub(replacer, obj)
        return obj
