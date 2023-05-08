import contextlib

from qgis.core import QgsApplication


@contextlib.contextmanager
def qgis_context(qgis_path):
    # Supply path to qgis install location
    QgsApplication.setPrefixPath(qgis_path, True)

    # Create a reference to the QgsApplication.  Setting the
    # second argument to False disables the GUI.
    qgs = QgsApplication([], False)
    qgs.initQgis()

    from providers import CustomAlgorithmProvider

    custom_provider = CustomAlgorithmProvider()
    custom_provider.loadAlgorithms()
    qgs.processingRegistry().addProvider(custom_provider)

    from processing.core.Processing import Processing
    from qgis import processing

    Processing.initialize()

    yield processing

    qgs.exitQgis()
