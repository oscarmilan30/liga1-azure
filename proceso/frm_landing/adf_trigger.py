"""
Actualiza los parámetros de ejecución en Azure SQL y dispara el pipeline
orquestador E2E en Azure Data Factory.

Uso:
    python adf_trigger.py --modo INCREMENTAL
    python adf_trigger.py --modo REPROCESO --anio 2023
    python adf_trigger.py --modo HISTORICO

Variables de entorno requeridas:
    AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
    ADF_SUBSCRIPTION_ID, ADF_RESOURCE_GROUP, ADF_FACTORY_NAME
    ADF_KV_URL, ADF_SQL_SERVER, ADF_SQL_DATABASE, ADF_SQL_USERNAME

Opcional:
    ADF_KV_SECRET_SQL_PASSWORD  nombre del secret KV con la password SQL
                                dev  → kv-sql-password (default)
                                prod → kv-sql-password-prod
"""

import os
import sys
import argparse
import requests
import pymssql
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient


MODOS_VALIDOS = {"INCREMENTAL", "REPROCESO", "HISTORICO"}
ADF_PIPELINE_NAME = "pl_Orchestrator_E2E_liga1"
ADF_API_VERSION = "2018-06-01"


def _credential():
    return ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )


def get_sql_password():
    kv_url      = os.environ["ADF_KV_URL"]
    secret_name = os.environ.get("ADF_KV_SECRET_SQL_PASSWORD", "kv-sql-password")
    client      = SecretClient(vault_url=kv_url, credential=_credential())
    password    = client.get_secret(secret_name).value
    print(f"✅ Password obtenido desde Key Vault ({kv_url}) secret={secret_name}")
    return password


def update_sql_params(sql_password: str, modo: str, anio: str | None):
    server = os.environ["ADF_SQL_SERVER"]
    database = os.environ["ADF_SQL_DATABASE"]
    username = os.environ["ADF_SQL_USERNAME"]

    conn = pymssql.connect(
        server=server,
        database=database,
        user=username,
        password=sql_password,
        login_timeout=30,
    )
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE dbo.tbl_pipeline_parametros SET Valor = %s "
        "WHERE PipelineId = 1 AND Parametro = 'MODO_EJECUCION'",
        (modo,),
    )
    print(f"✅ tbl_pipeline_parametros: MODO_EJECUCION = {modo}")

    if modo == "REPROCESO":
        if not anio:
            print("❌ --anio es obligatorio para modo REPROCESO")
            conn.close()
            sys.exit(1)
        cursor.execute(
            "UPDATE dbo.tbl_pipeline_parametros SET Valor = %s "
            "WHERE PipelineId = 1 AND Parametro = 'ANIO_REPROCESO'",
            (anio,),
        )
        print(f"✅ tbl_pipeline_parametros: ANIO_REPROCESO = {anio}")

    conn.commit()
    conn.close()


def trigger_adf():
    subscription_id = os.environ["ADF_SUBSCRIPTION_ID"]
    resource_group = os.environ["ADF_RESOURCE_GROUP"]
    factory_name = os.environ["ADF_FACTORY_NAME"]

    token = _credential().get_token("https://management.azure.com/.default").token

    url = (
        f"https://management.azure.com/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.DataFactory/factories/{factory_name}"
        f"/pipelines/{ADF_PIPELINE_NAME}/createRun"
        f"?api-version={ADF_API_VERSION}"
    )

    body = {
        "parameters": {
            "PipelineId": 35,
            "TablaParametro": "tbl_pipeline_parametros",
            "pipeline_type": "E2E",
            "RawPipelineId": 1,
        }
    }

    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )

    if not response.ok:
        print(f"❌ Error ADF REST API: {response.status_code} — {response.text}")
        sys.exit(1)

    run_id = response.json().get("runId")
    factory_path = (
        f"%2Fsubscriptions%2F{subscription_id}"
        f"%2FresourceGroups%2F{resource_group}"
        f"%2Fproviders%2FMicrosoft.DataFactory%2Ffactories%2F{factory_name}"
    )
    monitor_url = f"https://adf.azure.com/en/monitoring/pipelineruns/{run_id}?factory={factory_path}"
    print(f"✅ Pipeline ADF disparado exitosamente")
    print(f"   Factory : {factory_name}")
    print(f"   Pipeline: {ADF_PIPELINE_NAME}")
    print(f"   RunId   : {run_id}")
    print(f"   Monitor : {monitor_url}")
    return run_id


def main():
    parser = argparse.ArgumentParser(description="Dispara el pipeline E2E de ADF")
    parser.add_argument(
        "--modo",
        required=True,
        choices=["INCREMENTAL", "REPROCESO", "HISTORICO"],
        help="Modo de ejecución",
    )
    parser.add_argument(
        "--anio",
        default=None,
        help="Año a reprocesar (solo modo REPROCESO)",
    )
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  Liga 1 — Trigger ADF | Modo: {args.modo}")
    if args.anio:
        print(f"  Año: {args.anio}")
    print(f"{'='*55}\n")

    required_env = [
        "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET",
        "ADF_SUBSCRIPTION_ID", "ADF_RESOURCE_GROUP", "ADF_FACTORY_NAME",
        "ADF_KV_URL", "ADF_SQL_SERVER", "ADF_SQL_DATABASE", "ADF_SQL_USERNAME",
    ]
    # ADF_KV_SECRET_SQL_PASSWORD es opcional — default kv-sql-password
    missing = [v for v in required_env if not os.environ.get(v)]
    if missing:
        print(f"❌ Variables de entorno faltantes: {', '.join(missing)}")
        sys.exit(1)

    sql_password = get_sql_password()
    update_sql_params(sql_password, args.modo, args.anio)
    trigger_adf()

    print(f"\n✅ Proceso completado. ADF ejecutará el pipeline E2E en modo {args.modo}.\n")


if __name__ == "__main__":
    main()
