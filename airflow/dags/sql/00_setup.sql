-- Run once before the first DAG execution if the datasets do not already exist.
CREATE SCHEMA IF NOT EXISTS `{{ params.project_id }}.{{ params.raw_dataset }}`;
CREATE SCHEMA IF NOT EXISTS `{{ params.project_id }}.{{ params.stg_dataset }}`;
CREATE SCHEMA IF NOT EXISTS `{{ params.project_id }}.{{ params.mart_dataset }}`;
