import datetime
import sqlalchemy as sa

class TypeManager(object):
	type_dict = {}
	@classmethod
	def register_type(cls, sa_type, gitdb_type):
		cls.type_dict[sa_type] = gitdb_type


class AbstractType(object):
	@staticmethod
	def to_string(self, value):
		raise NotImplementedError()
	@staticmethod
	def from_string(self, value):
		raise NotImplementedError()

class String(AbstractType):
	@staticmethod
	def to_string(value):
		return value
	@staticmethod
	def from_string(value):
		return value
TypeManager.register_type(sa.String, String)

class Integer(AbstractType):
	@staticmethod
	def to_string(value):
		return str(value)
	@staticmethod
	def from_string(value):
		return int(value)
TypeManager.register_type(sa.Integer, Integer)

class DateTime(AbstractType):
	@staticmethod
	def to_string(value):
		return value.strftime('%Y-%m-%d_%H-%M-%S')
	@staticmethod
	def from_string(value):
		return datetime.datetime.strptime(value, '%Y-%m-%d_%H-%M-%S')
TypeManager.register_type(sa.DateTime, DateTime)

class Bool(AbstractType):
	@staticmethod
	def to_string(value):
		return str(value)
	@staticmethod
	def from_string(value):
		if value.lower() == 'true':
			return True
		elif value.lower() == 'false':
			return False
		elif value.lower() == 'none':
			return None
		else:
			raise ValueError('Invalid value for type Boolean: {0}'.format(value))
TypeManager.register_type(sa.Boolean, Bool)

