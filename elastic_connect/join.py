from abc import ABC
import importlib

class Join(ABC):
	"""Abstract parent of model joins - dependent child / parent models."""

	def __init__(self, source, target):
		self.source = source
		self._target = target
		self.target = None

	def get_target(self):
		"""Gets the target model of the join."""

		def class_for_name(module_name, class_name):
			"""Handles import of target model class. Needed to prevent circular dependency."""
			m = importlib.import_module(module_name)
			c = getattr(m, class_name)
			return c

		if not self.target:
			# get class from string - to avoid curcular imports
			self.target = class_for_name('models', self._target)
		return self.target


class SingleJoin(Join):
	"""1:1 model join."""

	pass


class MultiJoin(Join):
	"""1:N model join."""

	def __init__(self, source, target, join_by=None):
		super(MultiJoin, self).__init__(source=source, target=target)
		self.join_by = join_by or source._mapping['_name'] + '_id'