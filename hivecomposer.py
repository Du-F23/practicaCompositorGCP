#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example Airflow DAG for DataprocSubmitJobOperator with hive job.
"""
from __future__ import annotations

import os
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage


DAG_ID = "dataproc_hive"
storage_client = storage.Client()
PROJECT_ID = "graphite-prism-381016"


CLUSTER_NAME = "cluster-dataproc-hive"

REGION = "us-central1"

ZONE = "us-central1-c"



# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
   # "worker_config": {
    #  "num_instances": 2,
     #   "machine_type_uri": "n1-standard-4",
      #  "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    #},
}

# [END how_to_cloud_dataproc_create_cluster]

TIMEOUT = {"seconds": 1 * 24 * 60 * 60}


# [START how_to_cloud_dataproc_hive_config]
HIVE_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hive_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
}

HIVE_JOB_TWO = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hive_job": {"query_list": {"queries": ["CREATE DATABASE MIUSUARIO_TEST;"]}},
}

HIVE_JOB_TREE = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hive_job": {"query_list": {"queries": ["CREATE TABLE MIUSUARIO_TEST.PERSONA(ID STRING,NOMBRE STRING,TELEFONO STRING,CORREO STRING,FECHA_INGRESO STRING,EDAD INT,SALARIO DOUBLE,ID_EMPRESA STRING)ROW FORMAT DELIMITEDFIELDS TERMINATED BY '|'LINES TERMINATED BY '\n'STORED AS TEXTFILE;"]}},
}

HIVE_JOB_FOUR = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hive_job": {"query_list": {"queries": ["SHOW TABLES IN MIUSUARIO_TEST;"]}},
}



# [END how_to_cloud_dataproc_hive_config]


with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataproc"],
) as dag:
    # [START how_to_cloud_dataproc_create_cluster_operator]
        create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,

        )
    # [END how_to_cloud_dataproc_create_cluster_operator]

        hive_task = DataprocSubmitJobOperator(
        task_id="hive_task", job=HIVE_JOB, region=REGION, project_id=PROJECT_ID
        )
        hive_task_two = DataprocSubmitJobOperator(
        task_id="hive_create_database", job=HIVE_JOB_TWO, region=REGION, project_id=PROJECT_ID
        )
        hive_task_tree = DataprocSubmitJobOperator(
        task_id="hive_create_table", job=HIVE_JOB_TREE, region=REGION, project_id=PROJECT_ID
        )
        hive_task_four = DataprocSubmitJobOperator(
        task_id="hive_show_tables", job=HIVE_JOB_FOUR, region=REGION, project_id=PROJECT_ID
        )

    # [START how_to_cloud_dataproc_delete_cluster_operator]
        delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        )
    # [END how_to_cloud_dataproc_delete_cluster_operator]
        delete_cluster.trigger_rule = TriggerRule.ALL_DONE

        create_cluster >> hive_task >> hive_task_two >> hive_task_tree >> hive_task_four >> delete_cluster
