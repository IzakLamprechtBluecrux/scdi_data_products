"""
Generic YAML Data Contract Validator for Databricks / PySpark.

Location:
    scdi_data_products/contract_validator_generic/data_contract_validator.py

Purpose:
    Reusable validation engine that reads a YAML data contract and validates
    source tables and fields before transformation logic runs.

Supported validations:
    - Table existence
    - Field existence
    - Nullability
    - Regex checks
    - Exact length checks
    - Max length checks
    - Allowed values
    - Castability checks
    - Required reference values
    - Delta results logging
    - Pipeline failure on blocking errors
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml
except ImportError as exc:
    raise ImportError(
        "PyYAML is required. In Databricks, install it with: %pip install pyyaml"
    ) from exc

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


def load_yaml_contract(contract_file_path: str) -> Dict[str, Any]:
    """
    Load a YAML data contract from a local, Databricks repo, workspace, or mounted path.
    """
    with open(contract_file_path, "r", encoding="utf-8") as file:
        contract = yaml.safe_load(file)

    return contract or {}


def normalise_path(path: str) -> str:
    """
    Normalise Databricks paths.

    Example:
        dbfs:/FileStore/contracts/file.yml
        becomes:
        /dbfs/FileStore/contracts/file.yml
    """
    return path.replace("dbfs:/", "/dbfs/")


def safe_rule_id(*parts: Any) -> str:
    """
    Create a clean, consistent rule ID from multiple values.
    """
    return "_".join(
        str(part)
        .replace(".", "_")
        .replace("-", "_")
        .replace(" ", "_")
        .replace("/", "_")
        .upper()
        for part in parts
        if part is not None
    )


def quote_col(column_name: str) -> str:
    """
    Safely quote a Spark SQL column name.
    """
    escaped = column_name.replace("`", "``")
    return f"`{escaped}`"


class GenericDataContractValidator:
    """
    Generic YAML-driven data contract validator for Databricks / PySpark.
    """

    def __init__(
        self,
        spark: SparkSession,
        contract_file_path: str,
        validation_results_table: str = "workspace.default.data_contract_validation_results",
        fail_on_warning: bool = False,
        write_results: bool = True,
    ) -> None:
        self.spark = spark
        self.contract_file_path = normalise_path(contract_file_path)
        self.validation_results_table = validation_results_table
        self.fail_on_warning = fail_on_warning
        self.write_results = write_results

        self.contract = load_yaml_contract(self.contract_file_path)

        self.contract_id = self.contract.get("id", "unknown_contract")
        self.contract_version = str(self.contract.get("version", "unknown_version"))

        self.target_data_product = self.contract.get("execution", {}).get(
            "targetDataProduct",
            "unknown_target_data_product",
        )

        self.validation_results: List[Dict[str, Any]] = []

        self._table_exists_cache: Dict[str, bool] = {}
        self._table_columns_cache: Dict[str, List[str]] = {}
        self._actual_column_cache: Dict[str, str] = {}

    def add_result(
        self,
        rule_id: str,
        rule_type: str,
        status: str,
        severity: str,
        table_name: Optional[str] = None,
        field_name: Optional[str] = None,
        message: Optional[str] = None,
        failed_count: Optional[int] = None,
    ) -> None:
        """
        Add a single validation result record.
        """
        self.validation_results.append(
            {
                "contract_id": self.contract_id,
                "contract_version": self.contract_version,
                "target_data_product": self.target_data_product,
                "contract_file_path": self.contract_file_path,
                "validation_timestamp": datetime.now().isoformat(),
                "rule_id": rule_id,
                "rule_type": rule_type,
                "status": status,
                "severity": severity,
                "table_name": table_name,
