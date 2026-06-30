
# ============================================================
# Generic YAML Data Contract Validator for Databricks / PySpark
# ============================================================
#
# Purpose:
#   Reads a YAML data contract and validates source tables/fields
#   before running transformation logic.
#
# Supports:
#   - Table existence
#   - Field existence
#   - Nullability
#   - Regex checks
#   - Exact length checks
#   - Max length checks
#   - Allowed values
#   - Castability checks
#   - Required reference values
#   - Writes results to Delta
#   - Stops pipeline on blocking error failures
#
# ============================================================

import yaml
from datetime import datetime
from pyspark.sql import functions as F


# ============================================================
# 1. Runtime Parameters
# ============================================================

# Option 1: hardcode for testing
CONTRACT_FILE_PATH = "/Workspace/Shared/contracts/material_plant_contract.yaml"

# Option 2: use Databricks widgets in production
# dbutils.widgets.text("contract_file_path", "")
# CONTRACT_FILE_PATH = dbutils.widgets.get("contract_file_path")

VALIDATION_RESULTS_TABLE = "workspace.default.data_contract_validation_results"

DEFAULT_ERROR_SEVERITY = "error"
DEFAULT_WARNING_SEVERITY = "warning"


# ============================================================
# 2. Load YAML Contract
# ============================================================

def load_yaml_contract(file_path: str) -> dict:
    """
    Loads the YAML contract from a Databricks workspace/local path.
    """
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


contract = load_yaml_contract(CONTRACT_FILE_PATH)

CONTRACT_ID = contract.get("id", "unknown_contract")
CONTRACT_VERSION = contract.get("version", "unknown_version")

TARGET_DATA_PRODUCT = (
    contract
    .get("execution", {})
    .get("targetDataProduct", "unknown_target_data_product")
)


# ============================================================
# 3. Validation Result Helper
# ============================================================

validation_results = []


def add_result(
    rule_id: str,
    rule_type: str,
    status: str,
    severity: str,
    table_name: str = None,
    field_name: str = None,
    message: str = None,
    failed_count: int = None
):
    """
    Adds one validation result record.
    """
    validation_results.append({
        "contract_id": CONTRACT_ID,
        "contract_version": CONTRACT_VERSION,
        "target_data_product": TARGET_DATA_PRODUCT,
        "validation_timestamp": datetime.now().isoformat(),
        "rule_id": rule_id,
        "rule_type": rule_type,
        "status": status,
        "severity": severity,
        "table_name": table_name,
        "field_name": field_name,
        "message": message,
        "failed_count": failed_count
    })


# ============================================================
# 4. Generic Spark Helpers
# ============================================================

def table_exists(table_name: str) -> bool:
    """
    Checks whether a Spark/Databricks table exists and is accessible.
    """
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except Exception:
        return False


def get_table_columns(table_name: str) -> list:
    """
    Returns uppercase list of column names for a given table.
    """
    try:
        return [field.name.upper() for field in spark.table(table_name).schema.fields]
    except Exception:
        return []


def get_actual_column_name(table_name: str, expected_column: str) -> str:
    """
    Finds the real column name from the table schema, case-insensitively.
    Useful because YAML may define fields in uppercase/lowercase.
    """
    df = spark.table(table_name)

    for col_name in df.columns:
        if col_name.upper() == expected_column.upper():
            return col_name

    return expected_column


def safe_rule_id(*parts) -> str:
    """
    Creates a clean rule ID from multiple text parts.
    """
    return "_".join(
        str(part)
        .replace(".", "_")
        .replace("-", "_")
        .replace(" ", "_")
        .upper()
        for part in parts
        if part is not None
    )


# ============================================================
# 5. Table Existence Validation
# ============================================================

def validate_table_existence(contract: dict):
    models = contract.get("models", {})

    for model_name, model_config in models.items():
        table_name = model_config.get("physicalName")
        required = model_config.get("required", True)

        if not table_name:
            add_result(
                rule_id=safe_rule_id("MODEL", model_name, "MISSING_PHYSICAL_NAME"),
                rule_type="contract_definition",
                status="failed",
                severity="error",
                table_name=None,
                message=f"Model '{model_name}' does not have physicalName defined."
            )
            continue

        exists = table_exists(table_name)

        if exists:
            add_result(
                rule_id=safe_rule_id("TABLE_EXISTS", model_name),
                rule_type="table_exists",
                status="passed",
                severity="error" if required else "warning",
                table_name=table_name,
                message=f"Table exists: {table_name}"
            )
        else:
            add_result(
                rule_id=safe_rule_id("TABLE_EXISTS", model_name),
                rule_type="table_exists",
                status="failed",
                severity="error" if required else "warning",
                table_name=table_name,
                message=f"Table does not exist or is not accessible: {table_name}"
            )


# ============================================================
# 6. Field Existence Validation
# ============================================================

def validate_field_existence(contract: dict):
    models = contract.get("models", {})

    for model_name, model_config in models.items():
        table_name = model_config.get("physicalName")
        fields = model_config.get("fields", {})

        if not table_name or not table_exists(table_name):
            continue

        existing_columns = get_table_columns(table_name)

        for logical_field_name, field_config in fields.items():
            source_field = field_config.get("sourceField", logical_field_name)
            required = field_config.get("required", True)

            if source_field.upper() in existing_columns:
                add_result(
                    rule_id=safe_rule_id("FIELD_EXISTS", model_name, source_field),
                    rule_type="field_exists",
                    status="passed",
                    severity="error" if required else "warning",
                    table_name=table_name,
                    field_name=source_field,
                    message=f"Field exists: {table_name}.{source_field}"
                )
            else:
                add_result(
                    rule_id=safe_rule_id("FIELD_EXISTS", model_name, source_field),
                    rule_type="field_exists",
                    status="failed",
                    severity="error" if required else "warning",
                    table_name=table_name,
                    field_name=source_field,
                    message=f"Field missing: {table_name}.{source_field}"
                )


# ============================================================
# 7. Nullability Validation
# ============================================================

def validate_nullability(contract: dict):
    models = contract.get("models", {})

    for model_name, model_config in models.items():
        table_name = model_config.get("physicalName")
        fields = model_config.get("fields", {})

        if not table_name or not table_exists(table_name):
            continue

        existing_columns = get_table_columns(table_name)

        for logical_field_name, field_config in fields.items():
            source_field = field_config.get("sourceField", logical_field_name)
            nullable = field_config.get("nullable", True)

            if nullable is True:
                continue

            if source_field.upper() not in existing_columns:
                continue

            actual_col = get_actual_column_name(table_name, source_field)
            df = spark.table(table_name)

            failed_count = df.filter(F.col(actual_col).isNull()).count()
            status = "passed" if failed_count == 0 else "failed"

            add_result(
                rule_id=safe_rule_id("NOT_NULL", model_name, source_field),
                rule_type="not_null",
                status=status,
                severity="error",
                table_name=table_name,
                field_name=source_field,
                failed_count=failed_count,
                message=f"Not-null check for {table_name}.{source_field}. Failed records: {failed_count}"
            )


# ============================================================
# 8. Regex Format Validation
# ============================================================

def validate_regex(contract: dict):
    models = contract.get("models", {})

    for model_name, model_config in models.items():
        table_name = model_config.get("physicalName")
        fields = model_config.get("fields", {})

        if not table_name or not table_exists(table_name):
            continue

        existing_columns = get_table_columns(table_name)

        for logical_field_name, field_config in fields.items():
            source_field = field_config.get("sourceField", logical_field_name)

            format_config = field_config.get("format", {})
            regex = format_config.get("regex")

            if not regex:
                continue

            if source_field.upper() not in existing_columns:
                continue

            actual_col = get_actual_column_name(table_name, source_field)
            df = spark.table(table_name)

            severity = field_config.get("severity", format_config.get("severity", "error"))

            failed_count = (
                df
                .filter(
                    F.col(actual_col).isNotNull()
                    & (~F.col(actual_col).cast("string").rlike(regex))
                )
                .count()
            )

            status = "passed" if failed_count == 0 else "failed"

            add_result(
                rule_id=safe_rule_id("REGEX", model_name, source_field),
                rule_type="regex",
                status=status,
                severity=severity,
                table_name=table_name,
                field_name=source_field,
                failed_count=failed_count,
                message=f"Regex check for {table_name}.{source_field}. Regex: {regex}. Failed records: {failed_count}"
            )


# ============================================================
# 9. Length Validation
# ============================================================

def validate_length(contract: dict):
    models = contract.get("models", {})

    for model_name, model_config in models.items():
        table_name = model_config.get("physicalName")
        fields = model_config.get("fields", {})

        if not table_name or not table_exists(table_name):
            continue

        existing_columns = get_table_columns(table_name)

        for logical_field_name, field_config in fields.items():
            source_field = field_config.get("sourceField", logical_field_name)
            format_config = field_config.get("format", {})

            exact_length = format_config.get("length")
            max_length = format_config.get("maxLength")

            if source_field.upper() not in existing_columns:
                continue

            actual_col = get_actual_column_name(table_name, source_field)
            df = spark.table(table_name)

            severity = field_config.get("severity", format_config.get("severity", "error"))

            if exact_length is not None:
                failed_count = (
                    df
                    .filter(
                        F.col(actual_col).isNotNull()
                        & (F.length(F.col(actual_col).cast("string")) != int(exact_length))
                    )
                    .count()
                )

                status = "passed" if failed_count == 0 else "failed"

                add_result(
                    rule_id=safe_rule_id("EXACT_LENGTH", model_name, source_field),
                    rule_type="exact_length",
                    status=status,
                    severity=severity,
                    table_name=table_name,
                    field_name=source_field,
                    failed_count=failed_count,
                    message=f"Exact length check for {table_name}.{source_field}. Expected length: {exact_length}. Failed records: {failed_count}"
                )

            if max_length is not None:
                failed_count = (
                    df
                    .filter(
                        F.col(actual_col).isNotNull()
                        & (F.length(F.col(actual_col).cast("string")) > int(max_length))
                    )
                    .count()
                )

                status = "passed" if failed_count == 0 else "failed"

                add_result(
                    rule_id=safe_rule_id("MAX_LENGTH", model_name, source_field),
                    rule_type="max_length",
                    status=status,
                    severity=severity,
                    table_name=table_name,
                    field_name=source_field,
                    failed_count=failed_count,
                    message=f"Max length check for {table_name}.{source_field}. Max length: {max_length}. Failed records: {failed_count}"
                )


# ============================================================
# 10. Allowed Values Validation
# ============================================================

def validate_allowed_values(contract: dict):
    models = contract.get("models", {})

    for model_name, model_config in models.items():
        table_name = model_config.get("physicalName")
        fields = model_config.get("fields", {})

        if not table_name or not table_exists(table_name):
            continue

        existing_columns = get_table_columns(table_name)

        for logical_field_name, field_config in fields.items():
            source_field = field_config.get("sourceField", logical_field_name)
            allowed_values = field_config.get("allowedValues")

            if not allowed_values:
                continue

            if source_field.upper() not in existing_columns:
                continue

            actual_col = get_actual_column_name(table_name, source_field)
            df = spark.table(table_name)

            severity = field_config.get("severity", "warning")

            failed_count = (
                df
                .filter(
                    F.col(actual_col).isNotNull()
                    & (~F.col(actual_col).cast("string").isin([str(v) for v in allowed_values]))
                )
                .count()
            )

            status = "passed" if failed_count == 0 else "failed"

            add_result(
                rule_id=safe_rule_id("ALLOWED_VALUES", model_name, source_field),
                rule_type="allowed_values",
                status=status,
                severity=severity,
                table_name=table_name,
                field_name=source_field,
                failed_count=failed_count,
                message=f"Allowed values check for {table_name}.{source_field}. Allowed: {allowed_values}. Failed records: {failed_count}"
            )


# ============================================================
# 11. Castability Validation
# ============================================================

def validate_castability(contract: dict):
    models = contract.get("models", {})

    for model_name, model_config in models.items():
        table_name = model_config.get("physicalName")
        fields = model_config.get("fields", {})

        if not table_name or not table_exists(table_name):
            continue

        existing_columns = get_table_columns(table_name)

        for logical_field_name, field_config in fields.items():
            source_field = field_config.get("sourceField", logical_field_name)

            format_config = field_config.get("format", {})
            castable_to = format_config.get("castableTo")

            if not castable_to:
                continue

            if source_field.upper() not in existing_columns:
                continue

            actual_col = get_actual_column_name(table_name, source_field)
            df = spark.table(table_name)

            severity = field_config.get("severity", format_config.get("severity", "error"))

            failed_count = (
                df
                .filter(
                    F.col(actual_col).isNotNull()
                    & F.expr(f"try_cast(`{actual_col}` as {castable_to})").isNull()
                )
                .count()
            )

            status = "passed" if failed_count == 0 else "failed"

            add_result(
                rule_id=safe_rule_id("CASTABLE_TO", castable_to, model_name, source_field),
                rule_type="castable_to_type",
                status=status,
                severity=severity,
                table_name=table_name,
                field_name=source_field,
                failed_count=failed_count,
                message=f"Castability check for {table_name}.{source_field}. Target type: {castable_to}. Failed records: {failed_count}"
            )


# ============================================================
# 12. Reference Data Required Values Validation
# ============================================================

def validate_reference_data_checks(contract: dict):
    reference_checks = contract.get("referenceDataChecks", [])

    for check in reference_checks:
        rule_id = check.get("ruleId", "REFERENCE_DATA_CHECK")
        table_name = check.get("table")
        field_name = check.get("field")
        required_values = check.get("requiredValues", [])
        severity = check.get("severity", "error")

        if not table_name or not field_name:
            add_result(
                rule_id=rule_id,
                rule_type="reference_data_check",
                status="failed",
                severity="error",
                table_name=table_name,
                field_name=field_name,
                message="Reference data check is missing table or field."
            )
            continue

        if not table_exists(table_name):
            add_result(
                rule_id=rule_id,
                rule_type="reference_data_check",
                status="failed",
                severity="error",
                table_name=table_name,
                field_name=field_name,
                message=f"Reference table does not exist: {table_name}"
            )
            continue

        existing_columns = get_table_columns(table_name)

        if field_name.upper() not in existing_columns:
            add_result(
                rule_id=rule_id,
                rule_type="reference_data_check",
                status="failed",
                severity="error",
                table_name=table_name,
                field_name=field_name,
                message=f"Reference field does not exist: {table_name}.{field_name}"
            )
            continue

        actual_col = get_actual_column_name(table_name, field_name)
        df = spark.table(table_name)

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

        add_result(
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
            )
        )


# ============================================================
# 13. Run All Validations
# ============================================================

validate_table_existence(contract)
validate_field_existence(contract)
validate_nullability(contract)
validate_regex(contract)
validate_length(contract)
validate_allowed_values(contract)
validate_castability(contract)
validate_reference_data_checks(contract)


# ============================================================
# 14. Create Results DataFrame
# ============================================================

results_df = spark.createDataFrame(validation_results)

display(results_df)


# ============================================================
# 15. Save Results to Delta Table
# ============================================================

(
    results_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(VALIDATION_RESULTS_TABLE)
)


# ============================================================
# 16. Quality Gate
# ============================================================

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

print("============================================================")
print("DATA CONTRACT VALIDATION SUMMARY")
print("============================================================")
print(f"Contract ID:          {CONTRACT_ID}")
print(f"Contract Version:     {CONTRACT_VERSION}")
print(f"Target Data Product:  {TARGET_DATA_PRODUCT}")
print(f"Contract File:        {CONTRACT_FILE_PATH}")
print(f"Results Table:        {VALIDATION_RESULTS_TABLE}")
print(f"Validation Time:      {datetime.now().isoformat()}")
print(f"Blocking Failures:    {blocking_failure_count}")
print(f"Warnings:             {warning_failure_count}")
print("============================================================")

if warning_failure_count > 0:
    print("WARNING FAILURES FOUND")
    display(warning_failures_df)

if blocking_failure_count > 0:
    print("DATA CONTRACT STATUS: FAILED")
    print("Transformation must NOT run.")
    display(blocking_failures_df)

    raise Exception(
        f"Data contract validation failed with {blocking_failure_count} blocking error(s). "
        f"Transformation for {TARGET_DATA_PRODUCT} has been stopped."
    )

else:
    print("DATA CONTRACT STATUS: PASSED")
    print("Transformation may now run.")

    # Optional if used in a Databricks workflow:
    # dbutils.notebook.exit("PASSED")
