import processing
from qgis.core import (
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingMultiStepFeedback,
    QgsProcessingParameterFeatureSink,
    QgsProcessingParameterVectorLayer,
)


class Nta_residential(QgsProcessingAlgorithm):
    def initAlgorithm(self, config=None):
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                "nta", "nta", types=[QgsProcessing.TypeVectorPolygon], defaultValue=None
            )
        )
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                "subway_stations",
                "subway_stations",
                types=[QgsProcessing.TypeVectorPoint],
                defaultValue=None,
            )
        )
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                "annual_turnstile_means",
                "annual_turnstile_means",
                types=[QgsProcessing.TypeVector],
                defaultValue=None,
            )
        )
        self.addParameter(
            QgsProcessingParameterFeatureSink(
                "Nta_residential",
                "nta_residential",
                type=QgsProcessing.TypeVectorAnyGeometry,
                createByDefault=True,
                supportsAppend=True,
                defaultValue=None,
            )
        )

    def processAlgorithm(self, parameters, context, model_feedback):
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        feedback = QgsProcessingMultiStepFeedback(4, model_feedback)
        results = {}
        outputs = {}

        # Join attributes by location
        alg_params = {
            "DISCARD_NONMATCHING": False,
            "INPUT": parameters["nta"],
            "JOIN": parameters["subway_stations"],
            "JOIN_FIELDS": ["complex_id"],
            "METHOD": 0,  # Create separate feature for each matching feature (one-to-many)
            "PREDICATE": [0],  # intersect
            "PREFIX": "",
            "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT,
        }
        outputs["JoinAttributesByLocation"] = processing.run(
            "native:joinattributesbylocation",
            alg_params,
            context=context,
            feedback=feedback,
            is_child_algorithm=True,
        )

        feedback.setCurrentStep(1)
        if feedback.isCanceled():
            return {}

        # Join attributes by field value
        alg_params = {
            "DISCARD_NONMATCHING": False,
            "FIELD": "complex_id",
            "FIELDS_TO_COPY": ["mean_daily_exits", "mean_daily_entries"],
            "FIELD_2": "complex_id",
            "INPUT": outputs["JoinAttributesByLocation"]["OUTPUT"],
            "INPUT_2": parameters["annual_turnstile_means"],
            "METHOD": 1,  # Take attributes of the first matching feature only (one-to-one)
            "PREFIX": "",
            "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT,
        }
        outputs["JoinAttributesByFieldValue"] = processing.run(
            "native:joinattributestable",
            alg_params,
            context=context,
            feedback=feedback,
            is_child_algorithm=True,
        )

        feedback.setCurrentStep(2)
        if feedback.isCanceled():
            return {}

        # Execute SQL
        alg_params = {
            "INPUT_DATASOURCES": outputs["JoinAttributesByFieldValue"]["OUTPUT"],
            "INPUT_GEOMETRY_CRS": None,
            "INPUT_GEOMETRY_FIELD": "",
            "INPUT_GEOMETRY_TYPE": None,
            "INPUT_QUERY": " WITH no_dups AS\n  (\n SELECT nta2020, complex_id, AVG(mean_daily_entries) as mean_daily_entries, AVG(mean_daily_exits) as mean_daily_exits, geometry\n FROM input1\n GROUP BY nta2020, complex_id, geometry\n  )\n  SELECT \n  nta2020, SUM(mean_daily_entries) AS mean_daily_entries, SUM(mean_daily_exits) AS mean_daily_exits, geometry\n  FROM no_dups\n  GROUP BY nta2020, geometry\n",
            "INPUT_UID_FIELD": "",
            "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT,
        }
        outputs["ExecuteSql"] = processing.run(
            "qgis:executesql",
            alg_params,
            context=context,
            feedback=feedback,
            is_child_algorithm=True,
        )

        feedback.setCurrentStep(3)
        if feedback.isCanceled():
            return {}

        # Field calculator
        alg_params = {
            "FIELD_LENGTH": 0,
            "FIELD_NAME": "is_residential",
            "FIELD_PRECISION": 0,
            "FIELD_TYPE": 6,  # Boolean
            "FORMULA": " if((mean_daily_entries > mean_daily_exits) or mean_daily_entries IS NULL, true, false)\n",
            "INPUT": outputs["ExecuteSql"]["OUTPUT"],
            "OUTPUT": parameters["Nta_residential"],
        }
        outputs["FieldCalculator"] = processing.run(
            "native:fieldcalculator",
            alg_params,
            context=context,
            feedback=feedback,
            is_child_algorithm=True,
        )
        results["Nta_residential"] = outputs["FieldCalculator"]["OUTPUT"]
        return results

    def name(self):
        return "nta_residential"

    def displayName(self):
        return "nta_residential"

    def group(self):
        return "Custom scripts"

    def groupId(self):
        return "customScripts"

    def shortHelpString(self):
        return "Calculate residential boolean for NTAs"

    def createInstance(self):
        return Nta_residential()
