import re
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date


class DataContractValidationError(Exception):
    pass


class DataContractValidator:
    def __init__(self, spark: SparkSession, contract_path: str):
        self.spark = spark
        self.contract_path = contract_path
        self.contract = self._load_contract()
        self.params = self.contract.get("runtime", {}).get("parameters", {})

    def _load_contract(self):
        with open(self.contract_path, "r") as file:
            return yaml.safe_load(file)

    def _resolve_value(self, value):
        if isinstance(value, str):
            for key, param_value in self.params.items():
                value = value.replace("${" + key + "}", str(param_value))
        return value

    def _resolve_list(self, values):
        return [self._resolve_value(v) for v in values]

    def _table_exists(self, table_name):
        try:
            return self.spark.catalog.tableExists(table_name)
        except Exception:
            try:
                self.spark.table(table_name).limit(1).collect()
                return True
            except Exception:
                return False

    def _get_columns(self, table_name):
        return [field.name for field in self.spark.table(table_name).schema.fields]

    def _result(self, table, check_type, status, message, severity="error", failed_count=None):
        return {
            "table": table,
            "check_type": check_type,
            "status": status,
            "severity": severity,
            "message": message,
            "failed_count": failed_count,
        }

    def validate(self):
        results = []

        for source in self.contract.get("sources", []):
            table = source["table"]

            for check in source.get("checks", []):
                check_type = check["type"]
                severity = check.get("severity", "error")

                try:
                    if check_type == "table_exists":
                        exists = self._table_exists(table)
                        results.append(
                            self._result(
                                table,
                                check_type,
                                "pass" if exists else "fail",
                                f"Table exists: {exists}",
                                severity,
                            )
                        )
                        if not exists:
                            break

                    elif check_type == "row_count_min":
                        min_count = int(check["min"])
                        count = self.spark.table(table).count()
                        results.append(
                            self._result(
                                table,
                                check_type,
                                "pass" if count >= min_count else "fail",
                                f"Row count {count}, expected minimum {min_count}",
                                severity,
                                failed_count=0 if count >= min_count else min_count - count,
                            )
                        )

                    elif check_type == "required_columns":
                        expected = check["columns"]
                        actual = self._get_columns(table)
                        missing = [c for c in expected if c not in actual]
                        results.append(
                            self._result(
                                table,
                                check_type,
                                "pass" if not missing else "fail",
                                f"Missing columns: {missing}",
                                severity,
                                failed_count=len(missing),
                            )
                        )

                    elif check_type == "not_null":
                        df = self.spark.table(table)
                        for column_name in check["columns"]:
                            failed_count = df.filter(col(column_name).isNull()).count()
                            results.append(
                                self._result(
                                    table,
                                    f"{check_type}:{column_name}",
                                    "pass" if failed_count == 0 else "fail",
                                    f"Null count for {column_name}: {failed_count}",
                                    severity,
                                    failed_count,
                                )
                            )

                    elif check_type == "accepted_values":
                        df = self.spark.table(table)
                        column_name = check["column"]
                        values = self._resolve_list(check["values"])
                        failed_count = df.filter(~col(column_name).isin(values)).count()
                        results.append(
                            self._result(
                                table,
                                f"{check_type}:{column_name}",
                                "pass" if failed_count == 0 else "fail",
                                f"Rows outside accepted values for {column_name}: {failed_count}",
                                severity,
                                failed_count,
                            )
                        )

                    elif check_type == "type_castable":
                        df = self.spark.table(table)
                        target_type = check["target_type"]
                        for column_name in check["columns"]:
                            failed_count = (
                                df.filter(col(column_name).isNotNull())
                                  .filter(col(column_name).cast(target_type).isNull())
                                  .count()
                            )
                            results.append(
                                self._result(
                                    table,
                                    f"{check_type}:{column_name}",
                                    "pass" if failed_count == 0 else "fail",
                                    f"Rows not castable to {target_type} for {column_name}: {failed_count}",
                                    severity,
                                    failed_count,
                                )
                            )

                    elif check_type == "freshness":
                        df = self.spark.table(table)
                        column_name = check["column"]
                        cutoff_date = self._resolve_value(check["cutoff_date"])
                        fresh_count = (
                            df.filter(to_date(col(column_name)) >= lit(cutoff_date))
                              .count()
                        )
                        results.append(
                            self._result(
                                table,
                                f"{check_type}:{column_name}",
                                "pass" if fresh_count > 0 else "fail",
                                f"Rows on or after {cutoff_date}: {fresh_count}",
                                severity,
                                failed_count=0 if fresh_count > 0 else 1,
                            )
                        )

                    else:
                        results.append(
                            self._result(
                                table,
                                check_type,
                                "fail",
                                f"Unsupported check type: {check_type}",
                                severity,
                            )
                        )

                except Exception as exc:
                    results.append(
                        self._result(
                            table,
                            check_type,
                            "fail",
                            f"Exception while running check: {str(exc)}",
                            severity,
                        )
                    )

        results.extend(self._run_cross_source_checks())
        return results

    def _run_cross_source_checks(self):
        results = []

        for check in self.contract.get("cross_source_checks", []):
            check_type = check["type"]
            severity = check.get("severity", "error")

            if check_type != "referential_integrity":
                results.append(
                    self._result(
                        check.get("left_table"),
                        check_type,
                        "fail",
                        f"Unsupported cross-source check type: {check_type}",
                        severity,
                    )
                )
                continue

            left_table = check["left_table"]
            right_table = check["right_table"]
            join_columns = check["join_columns"]
            left_filter = self._resolve_value(check.get("left_filter", "1 = 1"))
            max_orphan_count = int(check.get("max_orphan_count", 0))

            left_df = self.spark.table(left_table).filter(left_filter).alias("l")
            right_df = self.spark.table(right_table).alias("r")

            join_expr = [
                col(f"l.{join_col}") == col(f"r.{join_col}")
                for join_col in join_columns
            ]

            orphan_count = (
                left_df.join(right_df, join_expr, "left_anti")
                       .count()
            )

            results.append(
                self._result(
                    left_table,
                    check["name"],
                    "pass" if orphan_count <= max_orphan_count else "fail",
                    f"Orphan count from {left_table} to {right_table}: {orphan_count}",
                    severity,
                    orphan_count,
                )
            )

        return results

    def assert_passed(self, results):
        blocking_failures = [
            r for r in results
            if r["status"] == "fail" and r["severity"] == "error"
        ]

        if blocking_failures:
            raise DataContractValidationError(
                f"Data contract failed with {len(blocking_failures)} blocking error(s)."
            )
