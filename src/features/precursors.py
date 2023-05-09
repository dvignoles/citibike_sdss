"""Precursor metrics (Reasonable walking distance, residential neighborhood, etc...)"""
import util


def residential_neighborhood(output_gpkg, project_dir):
    with util.qgis_context("/usr") as processing:
        output_gpkg = project_dir / "data/interim/precursors.gpkg"
        processing.run(
            "custom_provider:nta_residential",
            {
                "annual_turnstile_means": str(
                    project_dir
                    / "data/prepared/mta_2023.gpkg|layername=annual_morning_peak_complex"
                ),
                "nta": str(project_dir / "data/prepared/open_data.gpkg|layername=nta"),
                "subway_stations": str(
                    project_dir / "data/prepared/mta_2023.gpkg|layername=stations"
                ),
                "Nta_residential": f"ogr:dbname='{output_gpkg}' table=\"nta_residential\" (geom)",
            },
        )
