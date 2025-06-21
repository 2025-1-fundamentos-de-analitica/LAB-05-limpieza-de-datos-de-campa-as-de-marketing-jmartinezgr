"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import zipfile
from pathlib import Path


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortgage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    input_dir = Path("files/input")
    output_dir = Path("files/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    client_data = []
    campaign_data = []
    economics_data = []

    for zip_path in input_dir.glob("*.csv.zip"):
        with zipfile.ZipFile(zip_path, "r") as zipf:
            for file_name in zipf.namelist():
                with zipf.open(file_name) as f:
                    df = pd.read_csv(f)

                    cols = set(df.columns)
                    if {"client_id", "job", "education"}.issubset(cols):
                        client_data.append(df)
                    if {"number_contacts", "campaign_outcome"}.intersection(cols):
                        campaign_data.append(df)
                    if {"cons_price_idx", "euribor_three_months"}.issubset(cols):
                        economics_data.append(df)

    # CLIENT CSV
    if client_data:
        client_df = pd.concat(client_data, ignore_index=True)
        client_df["job"] = (
            client_df["job"]
            .str.replace(".", "", regex=False)
            .str.replace("-", "_", regex=False)
        )
        client_df["education"] = client_df["education"].str.replace(
            ".", "_", regex=False
        )
        client_df["education"] = client_df["education"].replace("unknown", pd.NA)
        client_df["credit_default"] = client_df["credit_default"].apply(
            lambda x: 1 if str(x).strip().lower() == "yes" else 0
        )

        # ⚠️ CORREGIR AQUÍ: debe ser "mortgage", no "mortage"
        client_df["mortgage"] = client_df["mortgage"].apply(
            lambda x: 1 if str(x).strip().lower() == "yes" else 0
        )

        client_df = client_df[
            [
                "client_id",
                "age",
                "job",
                "marital",
                "education",
                "credit_default",
                "mortgage",
            ]
        ]
        client_df.to_csv(output_dir / "client.csv", index=False)

    # CAMPAIGN CSV
    if campaign_data:
        campaign_df = pd.concat(campaign_data, ignore_index=True)

        if "previous_campaign_contacts" not in campaign_df.columns:
            if "previous_campaing_contacts" in campaign_df.columns:
                campaign_df.rename(
                    columns={
                        "previous_campaing_contacts": "previous_campaign_contacts"
                    },
                    inplace=True,
                )
            else:
                campaign_df["previous_campaign_contacts"] = 0

        for col in [
            "contact_duration",
            "previous_outcome",
            "campaign_outcome",
            "day",
            "month",
        ]:
            if col not in campaign_df.columns:
                campaign_df[col] = (
                    0
                    if col
                    in ["contact_duration", "previous_outcome", "campaign_outcome"]
                    else "01"
                )

        campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(
            lambda x: 1 if str(x).strip().lower() == "success" else 0
        )
        campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(
            lambda x: 1 if str(x).strip().lower() == "yes" else 0
        )

        month_map = {
            "jan": "01",
            "feb": "02",
            "mar": "03",
            "apr": "04",
            "may": "05",
            "jun": "06",
            "jul": "07",
            "aug": "08",
            "sep": "09",
            "oct": "10",
            "nov": "11",
            "dec": "12",
        }

        campaign_df["month"] = (
            campaign_df["month"].astype(str).str.strip().str.lower().map(month_map)
        )
        campaign_df["day"] = campaign_df["day"].astype(str).str.zfill(2)
        campaign_df["last_contact_date"] = (
            "2022-" + campaign_df["month"] + "-" + campaign_df["day"]
        )

        campaign_df = campaign_df[
            [
                "client_id",
                "number_contacts",
                "contact_duration",
                "previous_campaign_contacts",
                "previous_outcome",
                "campaign_outcome",
                "last_contact_date",
            ]
        ]
        campaign_df.to_csv(output_dir / "campaign.csv", index=False)

    # ECONOMICS CSV
    if economics_data:
        economics_df = pd.concat(economics_data, ignore_index=True)
        economics_df = economics_df[
            ["client_id", "cons_price_idx", "euribor_three_months"]
        ]
        economics_df.to_csv(output_dir / "economics.csv", index=False)


if __name__ == "__main__":
    clean_campaign_data()
