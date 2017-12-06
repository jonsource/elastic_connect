from distutils.core import setup

setup(
    name='elastic_connect',
    version='0.1dev',
    author='Jan Sourek',
    author_email='jan.sourek@gmail.com',
    packages=['elastic_connect',],
    license='MIT',
    description='Elasticsearch "ORM"',
    install_requires=['elasticsearch']
)
