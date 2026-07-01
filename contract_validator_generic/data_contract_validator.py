"""
Generic YAML Data Contract Validator for Databricks / PySpark

Location:
    scdi_data_products/contract_validator_generic/generic_data_contract_validator.py

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

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)


def load_yaml_contract(contract_file_path: str) -> Dict[str, Any]:
    """
    Load a YAML data contract from a local, Databricks repo, workspace, or mounted path.
    """
    with open(contract_file_path, "r") as file:
        return yaml.safe_load(file)


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
    ):
        self.spark = spark
        self.contract_file_path = normalise_path(contract_file_path)
        self.validation_results_table = validation_results_table
        self.fail_on_warning = fail_on_warning
        self.write_results = write_results

        self.contract = load_yaml_contract(self.contract_file_path)

        self.contract_id = self.contract.get("id", "unknown_contract")
        self.contract_version = str(self.contract.get("version", "unknown_version"))

        self.target_data_product = (
            self.contract
            .get("execution", {})
            .get("targetDataProduct", "unknown_target_data_product")
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
                "field_name": field_name,
                "message": message,
                "failed_count": failed_count,
            }
        )

    def create_empty_results_df(self) -> DataFrame:
        """
        Create an empty validation result DataFrame.
        """
        schema = StructType(
            [
                StructField("contract_id", StringType(), True),
                StructField("contract_version", StringType(), True),
                StructField("target_data_product", StringType(), True),
                StructField("contract_file_path", StringType(), True),
                StructField("validation_timestamp", StringType(), True),
                StructField("rule_id", StringType(), True),
                StructField("rule_type", StringType(), True),
                StructField("status", StringType(), True),
                StructField("severity", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("field_name", StringType(), True),
                StructField("message", StringType(), True),
                StructField("failed_count", LongType(), True),
            ]
        )

        return self.spark.createDataFrame([], schema)

    def table_exists(self, table_name: str) -> bool:
        """
        Check whether a Spark table exists and is accessible.
        """
        if table_name in self._table_exists_cache:
            return self._table_exists_cache[table_name]

        try:
            self.spark.table(table_name).limit(0).collect()
            exists = True
        except Exception:
            exists = False

        self._table_exists_cache[table_name] = exists
        return exists

    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Return the uppercase column names for a table.
        """
        if table_name in self._table_columns_cache:
            return self._table_columns_cache[table_name]

        try:
            columns = [
                field.name.upper()
                for field in self.spark.table(table_name).schema.fields
            ]
        except Exception:
            columns = []

        self._table_columns_cache[table_name] = columns
        return columns

    def get_actual_column_name(self, table_name: str, expected_column: str) -> str:
        """
        Return the case-sensitive column name from the table schema.
        """
        cache_key = f"{table_name}.{expected_column.upper()}"

        if cache_key in self._actual_column_cache:
            return self._actual_column_cache[cache_key]

        df = self.spark.table(table_name)

        for column_name in df.columns:
            if column_name.upper() == expected_column.upper():
                self._actual_column_cache[cache_key] = column_name
                return column_name

        self._actual_column_cache[cache_key] = expected_column
        return expected_column

    def field_exists(self, table_name: str, field_name: str) -> bool:
        """
        Check whether a field exists in a table.
        """
        return field_name.upper() in self.get_table_columns(table_name)

    def validate_contract_definition(self) -> None:
        """
        Validate that the YAML contract has the minimum required structure.
        """
        if not self.contract_id or self.contract_id == "unknown_contract":
            self.add_result(
                rule_id="CONTRACT_ID_MISSING",
                rule_type="contract_definition",
                status="failed",
                severity="error",
                message="The contract is missing the required top-level field: id",
            )

        if "models" not in self.contract:
            self.add_result(
                rule_id="CONTRACT_MODELS_MISSING",
                rule_type="contract_definition",
                status="failed",
                severity="error",
                message="The contract is missing the required top-level section: models",
            )

    def validate_table_existence(self) -> None:
        """
        Validate that every required source table exists.
        """
        models = self.contract.get("models", {})

        for model_name, model_config in models.items():
            table_name = model_config.get("physicalName")
            required = model_config.get("required", True)
            severity = "error" if required else "warning"

            if not table_name:
                self.add_result(
                    rule_id=safe_rule_id("MODEL", model_name, "MISSING_PHYSICAL_NAME"),
                    rule_type="contract_definition",
                    status="failed",
                    severity="error",
                    message=f"Model '{model_name}' does not have physicalName defined.",
                )
                continue

            exists = self.table_exists(table_name)

            self.add_result(
                rule_id=safe_rule_id("TABLE_EXISTS", model_name),
                rule_type="table_exists",
                status="passed" if exists else "failed",
                severity=severity,
                table_name=table_name,
                message=(
                    f"Table exists: {table_name}"
                    if exists
                    else f"Table does not exist or is not accessible: {table_name}"
                ),
            )

    def validate_field_existence(self) -> None:
        """
        Validate that every required field exists in each source table.
        """
        models = self.contract.get("models", {})

        for model_name, model_config in models.items():
            table_name = model_config.get("physicalName")
            fields = model_config.get("fields", {})

            if not table_name or not self.table_exists(table_name):
                continue

            for logical_field_name, field_config in fields.items():
                source_field = field_config.get("sourceField", logical_field_name)
                required = field_config.get("required", True)
                severity = "error" if required else "warning"
                exists = self.field_exists(table_name, source_field)

                self.add_result(
                    rule_id=safe_rule_id("FIELD_EXISTS", model_name, source_field),
                    rule_type="field_exists",
                    status="passed" if exists else "failed",
                    severity=severity,
                    table_name=table_name,
                    field_name=source_field,
                    message=(
                        f"Field exists: {table_name}.{source_field}"
                        if exists
                        else f"Field missing: {table_name}.{source_field}"
                    ),
                )

    def validate_nullability(self) -> None:
        """
        Validate not-null constraints.
        """
        models = self.contract.get("models", {})

        for model_name, model_config in models.items():
            table_name = model_config.get("physicalName")
            fields = model_config.get("fields", {})

            if not table_name or not self.table_exists(table_name):
                continue

            for logical_field_name, field_config in fields.items():
                source_field = field_config.get("sourceField", logical_field_name)
                nullable = field_config.get("nullable", True)

                if nullable:
                    continue

                if not self.field_exists(table_name, source_field):
                    continue

                actual_col = self.get_actual_column_name(table_name, source_field)
                df = self.spark.table(table_name)

                failed_count = df.filter(F.col(actual_col).isNull()).count()
                status = "passed" if failed_count == 0 else "failed"

                self.add_result(
                    rule_id=safe_rule_id("NOT_NULL", model_name, source_field),
                    rule_type="not_null",
                    status=status,
                    severity="error",
                    table_name=table_name,
                    field_name=source_field,
                    failed_count=failed_count,
                    message=(
                        f"Not-null check for {table_name}.{source_field}. "
                        f"Failed records: {failed_count}"
                    ),
                )

    def validate_regex(self) -> None:
        """
        Validate regex format checks defined in the YAML contract.
        """
        models = self.contract.get("models", {})

        for model_name, model_config in 
            table_name = model_config.get("physicalName")
            fields = model_config.get("fields", {})

            if not table_name or not self.table_exists(table_name):
                continue

            for logical_field_name, field_config in fields.items():
                source_field = field_config.get("sourceField", logical_field_name)
                format_config = field_config.get("format", {})
                regex = format_config.get("regex")

                if not regex:
                    continue

                if not self.field_exists(table_name, source_field):
                    continue

                actual_col = self.get_actual_column_name(table_name, source_field)

                severity = field_config.get(
                    "severity",
                    format_config.get("severity", "error"),
                )

                df = self.spark.table(table_name)

                failed_count = (
                    df
                    .filter(
                        F.col(actual_col).isNotNull()
                        & (~F.col(actual_col).cast("string").rlike(regex))
                    )
                    .count()
                )

                status = "passed" if failed_count == 0 else "failed"

                self.add_result(
                    rule_id=safe_rule_id("REGEX", model_name, source_field),
                    rule_type="regex",
                    status=status,
                    severity=severity,
                    table_name=table_name,
                    field_name=source_field,
                    failed_count=failed_count,
                    message=(
                        f"Regex check for {table_name}.{source_field}. "
                        f"Regex: {regex}. Failed records: {failed_count}"
                    ),
                )

    def validate_length(self) -> None:
        """
        Validate exact length and maximum length rules.
        """
        models = self.contract.get("models", {})

        for model_name, model_config in models.items():
            table_name = model_config.get("physicalName")
            fields = model_config.get("fields", {})

            if not table_name or not self.table_exists(table_name):
                continue

            for logical_field_name, field_config in fields.items():
                source_field = field_config.get("sourceField", logical_field_name)
                format_config = field_config.get("format", {})

                exact_length = format_config.get("length")
                max_length = format_config.get("maxLength")

                if exact_length is None and max_length is None:
                    continue

                if not self.field_exists(table_name, source_field):
                    continue

                actual_col = self.get_actual_column_name(table_name, source_field)

                severity = field_config.get(
                    "severity",
                    format_config.get("severity", "error"),
                )

                df = self.spark.table(table_name)

                if exact_length is not None:
                    failed_count = (
                        df
                        .filter(
                            F.col(actual_col).isNotNull()
                            & (
                                F.length(F.col(actual_col).cast("string"))
                                != int(exact_length)
                            )
                        )
                        .count()
                    )

                    status = "passed" if failed_count == 0 else "failed"

                    self.add_result(
                        rule_id=safe_rule_id(
                            "EXACT_LENGTH",
                            model_name,
                            source_field,
                        ),
                        rule_type="exact_length",
                        status=status,
                        severity=severity,
                        table_name=table_name,
                        field_name=source_field,
                        failed_count=failed_count,
                        message=(
                            f"Exact length check for {table_name}.{source_field}. "
                            f"Expected length: {exact_length}. "
                            f"Failed records: {failed_count}"
                        ),
                    )

                if max_length is not None:
                    failed_count = (
                        df
                        .filter(
                            F.col(actual_col).isNotNull()
                            & (
                                F.length(F.col(actual_col).cast("string"))
                                > int(max_length)
                            )
                        )
                        .count()
                    )

                    status = "passed" if failed_count == 0 else "failed"

                    self.add_result(
                        rule_id=safe_rule_id(
                            "MAX_LENGTH",
                            model_name,
                            source_field,
                        ),
                        rule_type="max_length",
                        status=status,
                        severity=severity,
                        table_name=table_name,
                        field_name=source_field,
                        failed_count=failed_count,
                        message=(
                            f"Max length check for {table_name}.{source_field}. "
                            f"Max length: {max_length}. "
                            f"Failed records: {failed_count}"
                        ),
                    )

    def validate_allowed_values(self) -> None:
        """
        Validate allowed values defined in the YAML contract.
        """
        models = self.contract.get("models", {})

        for model_name, model_config in models.items():
            table_name = model_config.get("physicalName")
            fields = model_config.get("fields", {})

            if not table_name or not self.table_exists(table_name):
                continue

            for logical_field_name, field_config in fields.items():
                source_field = field_config.get("sourceField", logical_field_name)
                allowed_values = field_config.get("allowedValues")

                if not allowed_values:
                    continue

                if not self.field_exists(table_name, source_field):
                    continue

                actual_col = self.get_actual_column_name(table_name, source_field)
                severity = field_config.get("severity", "warning")
                allowed_values_as_strings = [str(value) for value in allowed_values]

                df = self.spark.table(table_name)

                failed_count = (
                    df
                    .filter(
                        F.col(actual_col).isNotNull()
                        & (
                            ~F.col(actual_col)
                            .cast("string")
                            .isin(allowed_values_as_strings)
                        )
                    )
                    .count()
                )

                status = "passed" if failed_count == 0 else "failed"

                self.add_result(
                    rule_id=safe_rule_id("ALLOWED_VALUES", model_name, source_field),
                    rule_type="allowed_values",
                    status=status,
                    severity=severity,
                    table_name=table_name,
                    field_name=source_field,
                    failed_count=failed_count,
                    message=(
                        f"Allowed values check for {table_name}.{source_field}. "
                        f"Allowed values: {allowed_values}. "
                        f"Failed records: {failed_count}"
                    ),
                )

    def validate_castability(self) -> None:
        """
        Validate fields that must be castable to a target datatype.
        """
        models = self.contract.get("models", {})

        for model_name, model_config in models.items():
            table_name = model_config.get("physicalName")
            fields = model_config.get("fields", {})

            if not table_name or not self.table_exists(table_name):
                continue

            for logical_field_name, field_config in fields.items():
                source_field = field_config.get("sourceField", logical_field_name)
                format_config = field_config.get("format", {})
                castable_to = format_config.get("castableTo")

                if not castable_to:
                    continue

                if not self.field_exists(table_name, source_field):
                    continue

                actual_col = self.get_actual_column_name(table_name, source_field)

                severity = field_config.get(
                    "severity",
                    format_config.get("severity", "error"),
                )

                df = self.spark.table(table_name)

                failed_count = (
                    df
                    .filter(
                        F.col(actual_col).isNotNull()
                        & F.expr(
                            f"try_cast({quote_col(actual_col)} as {castable_to})"
                        ).isNull()
                    )
                    .count()
                )

                status = "passed" if failed_count == 0 else "failed"

                self.add_result(
                    rule_id=safe_rule_id(
                        "CASTABLE_TO",
                        castable_to,
                        model_name,
                        source_field,
                    ),
                    rule_type="castable_to_type",
                    status=status,
                    severity=severity,
                    table_name=table_name,
                    field_name=source_field,
                    failed_count=failed_count,
                    message=(
                        f"Castability check for {table_name}.{source_field}. "
                        f"Target type: {castable_to}. "
                        f"Failed records: {failed_count}"
                    ),
                )

    def validate_reference_data_checks(self) -> None:
        """
        Validate required reference values.
        """
        reference_checks = self.contract.get("referenceDataChecks", [])

        for check in reference_checks:
            rule_id = check.get("ruleId", "REFERENCE_DATA_CHECK")
            table_name = check.get("table")
            field_name = check.get("field")
            required_values = check.get("requiredValues", [])
            severity = check.get("severity", "error")

            if not table_name or not field_name:
                self.add_result(
                    rule_id=rule_id,
                    rule_type="reference_data_check",
                    status="failed",
                    severity="error",
                    table_name=table_name,
                    field_name=field_name,
                    message="Reference data check is missing table or field.",
                )
                continue

            if not self.table_exists(table_name):
                self.add_result(
                    rule_id=rule_id,
                    rule_type="reference_data_check",
                    status="failed",
                    severity="error",
                    table_name=table_name,
                    field_name=field_name,
                    message=f"Reference table does not exist: {table_name}",
                )
                continue

            if not self.field_exists(table_name, field_name):
                self.add_result(
                    rule_id=rule_id,
                    rule_type="reference_data_check",
                    status="failed",
                    severity="error",
                    table_name=table_name,
                    field_name=field_name,
                    message=f"Reference field does not exist: {table_name}.{field_name}",
                )
                continue

            actual_col = self.get_actual_column_name(table_name, field_name)
            df = self.spark.table(table_name)

            existing_values = [
                row[actual_col]
                for row in df.select(actual_col).distinct().collect()
            ]

            missing_values = [
                value
                for value in required_values
                if value not in existing_values
            ]

            failed_count = len(missing_values)
            status = "passed" if failed_count == 0 else "failed"

            self.add_result(
                rule_id=rule_id,
                rule_type="required_values_exist",
                status=status,
                severity=severity,
                table_name=table_name,
                field_name=field_name,
                failed_count=failed_count,
                message=(
                    "All required reference values exist."
                    if failed_count == 0
                    else f"Missing required values: {missing_values}"
                ),
            )

    def run_validations(self) -> DataFrame:
        """
        Run all validation checks and return a results DataFrame.
        """
        self.validate_contract_definition()
        self.validate_table_existence()
        self.validate_field_existence()
        self.validate_nullability()
        self.validate_regex()
        self.validate_length()
        self.validate_allowed_values()
        self.validate_castability()
        self.validate_reference_data_checks()

        if self.validation_results:
            results_df = self.spark.createDataFrame(self.validation_results)
        else:
            results_df = self.create_empty_results_df()

        if self.write_results:
            (
                results_df
                .write
                .format("delta")
                .mode("append")
                .saveAsTable(self.validation_results_table)
            )

        return results_df

    def evaluate_quality_gate(self, results_df: DataFrame) -> Tuple[str, int, int]:
        """
        Evaluate whether the contract passed or failed.
        """
        blocking_failures_df = results_df.filter(
            (F.col("status") == "failed")
            & (F.col("severity") == "error")
        )

        warning_failures_df = results_df.filter(
            (F.col("status") == "failed")
            & (F.col("severity") == "warning")
        )

        blocking_failure_count = blocking_failures_df.count()
        warning_failure_count = warning_failures_df.count()

        if blocking_failure_count > 0:
            return "FAILED", blocking_failure_count, warning_failure_count

        if self.fail_on_warning and warning_failure_count > 0:
            return "FAILED", blocking_failure_count, warning_failure_count

        return "PASSED", blocking_failure_count, warning_failure_count

    def run(self) -> DataFrame:
        """
        Run validations, evaluate the quality gate, and fail the pipeline if needed.
        """
        results_df = self.run_validations()

        status, blocking_failure_count, warning_failure_count = (
            self.evaluate_quality_gate(results_df)
        )

        print("============================================================")
        print("DATA CONTRACT VALIDATION SUMMARY")
        print("============================================================")
        print(f"Contract ID:          {self.contract_id}")
        print(f"Contract Version:     {self.contract_version}")
        print(f"Target Data Product:  {self.target_data_product}")
        print(f"Contract File:        {self.contract_file_path}")
        print(f"Results Table:        {self.validation_results_table}")
        print(f"Validation Time:      {datetime.now().isoformat()}")
        print(f"Status:               {status}")
        print(f"Blocking Failures:    {blocking_failure_count}")
        print(f"Warnings:             {warning_failure_count}")
        print("============================================================")

        if status == "FAILED":
            raise Exception(
                "Data contract validation failed. "
                f"Contract: {self.contract_id}. "
                f"Blocking failures: {blocking_failure_count}. "
                f"Warnings: {warning_failure_count}. "
                f"Target data product: {self.target_data_product}."
            )

        return results_df


def run_contract_validation(
    spark: SparkSession,
    contract_file_path: str,
    validation_results_table: str = "workspace.default.data_contract_validation_results",
    fail_on_warning: bool = False,
    write_results: bool = True,
) -> DataFrame:
    """
    Public function used by Databricks runner scripts or notebooks.
    """
    validator = GenericDataContractValidator(
        spark=spark,
        contract_file_path=contract_file_path,
        validation_results_table=validation_results_table,
        fail_on_warning=fail_on_warning,
        write_results=write_results,
    )

    return validator.run()
