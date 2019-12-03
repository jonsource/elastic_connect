from distutils.core import setup

setup(
    name='elastic_connect',
    version='0.2.3',
    author='Jan Sourek',
    author_email='jan.sourek@gmail.com',
    packages=['elastic_connect', 'elastic_connect.data_types'],
    license='MIT',
    description='Elasticsearch "ORM"',
    install_requires=['elasticsearch', 'python-dateutil', 'requests']
)
