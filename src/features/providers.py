from algorithms import Nta_residential
from qgis.core import QgsProcessingProvider


class CustomAlgorithmProvider(QgsProcessingProvider):
    """QGIS provider for geoprocessing models run outside of QGIS"""

    def loadAlgorithms(self, *args, **kwargs):
        self.addAlgorithm(Nta_residential())

    def id(self, *args, **kwargs):
        """Used for identifying the provider.
        This string should be a unique, short, character only string,
        eg "qgis" or "gdal". This string should not be localised.
        """
        return "custom_provider"

    def name(self, *args, **kwargs):
        """
        This string should be as short as possible (e.g. "Lastools", not
        "Lastools version 1.0.1 64-bit") and localised.
        """
        return self.tr("Custom Provider")

    def icon(self):
        """Should return a QIcon which is used for your provider inside
        the Processing toolbox.
        """
        return QgsProcessingProvider.icon(self)
