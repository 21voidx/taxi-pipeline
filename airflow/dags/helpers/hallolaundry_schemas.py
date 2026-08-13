from __future__ import annotations

"""
Explicit BigQuery RAW schemas for the HalloLaundry pipeline.

Design:
- Schema is derived from the RAW fields referenced by haghi_laundry_datamart(2).sql.
- Scalar leaf values are stored as STRING in RAW.
- Objects remain RECORD.
- Arrays consumed with UNNEST in STG remain REPEATED RECORD.
- Customer address/outlet payloads are stored as JSON because the SQL only
  passes them through and does not define their internal structure.
- Source fields not listed here are intentionally ignored by the BigQuery
  load job. The complete source payload remains preserved in GCS NDJSON.
"""


def _string(name: str, mode: str = "NULLABLE") -> dict:
    return {"name": name, "type": "STRING", "mode": mode}


def _json(name: str, mode: str = "NULLABLE") -> dict:
    return {"name": name, "type": "JSON", "mode": mode}


def _record(name: str, fields: list[dict], mode: str = "NULLABLE") -> dict:
    return {
        "name": name,
        "type": "RECORD",
        "mode": mode,
        "fields": fields,
    }


def _metadata_fields() -> list[dict]:
    return [
        _string("_ingested_at"),
        _string("_ingestion_date"),
        _string("_airflow_run_id"),
        _string("_extract_mode"),
        _string("_extract_start_date"),
        _string("_extract_end_date"),
        _string("_source"),
    ]


RAW_SCHEMAS: dict[str, list[dict]] = {
    "raw_transaction": [
        _string("id"),
        _string("note_number"),
        _string("ref_id"),
        _string("company_id"),
        _string("company_customer_id"),
        _record(
            "customer",
            [
                _string("uuid"),
                _string("name"),
            ],
        ),
        _string("company_outlet_id"),
        _record("outlet", [_string("name")]),
        _string("user_employee_id"),
        _record(
            "cashier",
            [
                _string("id"),
                _string("name"),
            ],
        ),
        _string("transaction_type"),
        _string("payment_status"),
        _string("transaction_status"),
        _string("transaction_status_text"),
        _string("position"),
        _string("is_late"),
        _string("is_delivery"),
        _string("is_express"),
        _string("company_outlet_express_service_id"),
        _string("transaction_progress"),
        _string("workshop_progress"),
        _string("estimation_finish_at"),
        _string("created_at"),
        _string("updated_at"),
        *_metadata_fields(),
    ],

    "raw_customer": [
        _string("customer_id"),
        _string("customer_name"),
        _string("customer_code"),
        _string("honorific"),
        _string("is_membership"),
        _string("is_membership_deposit"),
        _string("is_active"),
        _string("phone"),
        _string("gender"),
        _string("customer_created_date"),
        _string("total_transaction_regular"),
        _string("total_transaction_self_service"),
        _json("detail_customer_addresses"),
        _json("detail_customer_outlets"),
        *_metadata_fields(),
    ],

    "raw_transaction_detail": [
        _string("id"),
        _string("note_number"),
        _string("ref_id"),
        _string("company_id"),
        _string("company_customer_id"),
        _record(
            "customer",
            [
                _string("uuid"),
                _string("name"),
                _string("is_membership_deposit"),
            ],
        ),
        _string("company_outlet_id"),
        _record("outlet", [_string("name")]),
        _string("user_employee_id"),
        _string("transaction_type"),
        _string("payment_status"),
        _string("transaction_status"),
        _string("transaction_status_text"),
        _string("position"),
        _string("is_late"),
        _string("is_delivery"),
        _string("company_outlet_express_service_id"),
        _string("transaction_progress"),
        _string("workshop_progress"),
        _string("created_at"),
        _string("updated_at"),
        _string("estimation_finish_at"),
        _string("request_cancelled_at"),
        _string("taking_at"),
        _string("cancelled_at"),
        _string("request_cancelled_reason"),

        _record(
            "regular",
            [
                _string("id"),
                _string("payment_type"),
                _string("amount"),
                _string("net_amount"),
                _string("net_amount_final"),
                _string("gross_income"),
                _string("discount"),
                _string("discount_regular_services"),
                _string("ppn"),
                _string("additional_price_amount"),
                _string("coin"),
                _string("amount_paid"),
                _string("amount_remaining"),
                _string("normal_price"),
                _string("express_price"),
                _record(
                    "transaction_services",
                    [
                        _string("id"),
                        _string("quantity"),
                        _string("amount"),
                        _string("discount"),
                        _string("net_amount"),
                        _string("sub_total"),
                        _string("created_at"),
                        _string("updated_at"),
                        _record(
                            "service",
                            [
                                _string("id"),
                                _string("name"),
                                _string("is_can_scanning"),
                                _string("multiply_scanning"),
                                _record(
                                    "category",
                                    [
                                        _string("id"),
                                        _string("name"),
                                    ],
                                ),
                                _record(
                                    "master_unit",
                                    [
                                        _string("id"),
                                        _string("name"),
                                    ],
                                ),
                            ],
                        ),
                    ],
                    mode="REPEATED",
                ),
            ],
        ),

        _record(
            "package",
            [
                _string("id"),
                _string("amount"),
                _string("net_amount"),
                _string("ppn"),
                _string("coin"),
                _record(
                    "transaction_services",
                    [
                        _string("id"),
                        _string("company_customer_deposit_package_id"),
                        _string("company_outlet_regular_service_deposit_id"),
                        _string("quantity"),
                        _string("amount"),
                        _string("created_at"),
                        _string("updated_at"),
                        _record(
                            "service",
                            [
                                _string("name"),
                                _string("quantity"),
                                _string("discount"),
                                _string("price"),
                                _string("base_price"),
                                _string("expired_in_days"),
                                _record(
                                    "regular_service",
                                    [
                                        _string("id"),
                                        _string("name"),
                                        _string("price"),
                                        _string("is_can_scanning"),
                                        _string("multiply_scanning"),
                                        _record(
                                            "category",
                                            [
                                                _string("id"),
                                                _string("name"),
                                            ],
                                        ),
                                        _record(
                                            "master_unit",
                                            [
                                                _string("id"),
                                                _string("name"),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                    mode="REPEATED",
                ),
            ],
        ),

        _record(
            "relation_services",
            [
                _string("id"),
                _string("transaction_service_id"),
                _string("transaction_type"),
                _string("company_outlet_regular_service_id"),
                _string("company_outlet_regular_service_deposit_id"),
                _string("master_unit_regular_service_id"),
                _string("estimation_finish_at"),
                _string("created_at"),
                _string("updated_at"),
                _record(
                    "service",
                    [
                        _string("name"),
                        _string("is_can_scanning"),
                        _string("multiply_scanning"),
                        _record("master_unit", [_string("name")]),
                        _record(
                            "regular_service",
                            [
                                _string("is_can_scanning"),
                                _string("multiply_scanning"),
                                _record("master_unit", [_string("name")]),
                            ],
                        ),
                    ],
                ),
                _record(
                    "rack",
                    [
                        _string("id"),
                        _string("combined_name"),
                        _string("number_rack"),
                    ],
                ),
            ],
            mode="REPEATED",
        ),

        # detail_payment is intentionally not part of the BQ RAW contract.
        # It is not referenced by the supplied STG/MART SQL and contains
        # polymorphic fields such as double_payments. The complete value is
        # still preserved in GCS NDJSON.
        *_metadata_fields(),
    ],

    "raw_transaction_self_service": [
        _string("id"),
        _string("note_number"),
        _string("ref_id"),
        _string("company_id"),
        _string("company_customer_id"),
        _string("company_outlet_id"),
        _record("company_outlet", [_string("name")]),
        _string("iot_machine_qr_code_id"),
        _string("status"),
        _string("transaction_type"),
        _string("total"),
        _string("fee_amount"),
        _string("total_paid"),
        _record(
            "payment",
            [
                _string("id"),
                _string("amount"),
                _string("channel_payment"),
                _string("brand_name"),
                _string("currency"),
                _string("rrn"),
                _string("buyer_ref"),
            ],
        ),
        _string("finished_at"),
        _string("created_at"),
        _string("updated_at"),
        _record(
            "services",
            [
                _string("id"),
                _string("iot_machine_id"),
                _string("iot_machine_service_id"),
                _string("service_type"),
                _string("quantity"),
                _string("amount"),
                _string("used_at"),
                _string("expired_at"),
                _string("created_at"),
                _string("updated_at"),
                _record(
                    "iot_machine",
                    [
                        _string("id"),
                        _string("name"),
                        _string("machine_type"),
                        _string("device_id"),
                        _string("machine_command_id"),
                        _string("machine_command_name"),
                    ],
                ),
                _record(
                    "machine_callback",
                    [
                        _string("status"),
                        _string("message"),
                    ],
                ),
            ],
            mode="REPEATED",
        ),
        *_metadata_fields(),
    ],

    "raw_deposit_purchase": [
        _string("id"),
        _string("note_number"),
        _string("ref_id"),
        _string("company_customer_id"),
        _record(
            "customer",
            [
                _string("uuid"),
                _string("name"),
            ],
        ),
        _string("company_outlet_id"),
        _record("outlet", [_string("name")]),
        _record(
            "cashier",
            [
                _string("id"),
                _string("name"),
            ],
        ),
        _string("master_payment_method_id"),
        _string("status"),
        _string("amount"),
        _string("net_amount"),
        _string("ppn_amount"),
        _string("created_at"),
        _string("request_cancelled_at"),
        _string("request_cancelled_reason"),
        _string("cancelled_at"),
        _string("updated_at"),
        _record(
            "deposit_services",
            [
                _string("id"),
                _string("company_outlet_regular_service_deposit_id"),
                _string("quantity_purchase"),
                _string("quantity_service"),
                _string("quantity_total"),
                _string("amount"),
                _string("discount"),
                _string("net_amount"),
                _string("total_amount"),
                _record(
                    "regular_service_deposit",
                    [
                        _string("name"),
                        _string("price"),
                    ],
                ),
            ],
            mode="REPEATED",
        ),
        *_metadata_fields(),
    ],

    "raw_history_scanning_iot": [
        _string("id"),
        _string("scanning_type"),
        _string("modelable_type"),
        _string("iot_machine_id"),
        _record(
            "iot_machine",
            [
                _string("id"),
                _string("name"),
                _string("machine_type"),
                _string("device_id"),
                _string("machine_command_id"),
                _string("machine_command_name"),
            ],
        ),
        _string("modelable_id"),
        _record(
            "modelable",
            [
                _string("id"),
                _string("transaction_relation_id"),
                _string("transaction_relation_service_id"),
                _record(
                    "transaction_relation",
                    [
                        _string("transaction_type"),
                        _string("payment_status"),
                        _string("transaction_status"),
                        _string("cancelled_at"),
                    ],
                ),
                _record(
                    "regular_service",
                    [
                        _string("id"),
                        _string("name"),
                        _string("multiply_scanning"),
                    ],
                ),
                _string("total_scanning"),
                _string("mode_id"),
            ],
        ),
        _record(
            "mode",
            [
                _string("name"),
                _string("timer"),
                _string("delay"),
            ],
        ),
        _record(
            "created_by",
            [
                _string("id"),
                _string("name"),
            ],
        ),
        _string("created_at"),
        _string("updated_at"),
        *_metadata_fields(),
    ],
}


def get_raw_schema(raw_table: str) -> list[dict]:
    try:
        return RAW_SCHEMAS[raw_table]
    except KeyError as exc:
        raise KeyError(f"No explicit RAW schema configured for {raw_table!r}.") from exc