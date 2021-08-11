from typing import Dict
from dq_whistler.data_sources.data_source import DataSource, DataSourceOptions

class FileSourceOptions(DataSourceOptions):
	"""
	Base class of options for file based source
	Args:
		source_options: Dict config for all source related options
	"""

	_source_options: Dict[str, str]

	def __init__(self, source_options: Dict[str, str]):
		super().__init__(source_options)
		_supported_options = ["path", "delimiter", "header", "infer_schema", "format"]
		_options = source_options.keys()
		if not all(key in _supported_options for key in source_options.keys()):
			raise ValueError(f"Supported keys are {_supported_options}")
		self._source_options = source_options



class FileSource(DataSource):
	"""
	Base class for file based source
	"""