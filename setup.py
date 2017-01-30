try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name='api_transilien_manager',
    packages=['src'],
    version='0.0.1',
    description='Task manager to exploit data from transilien\'s api',
    author='Leonard Binet',
    author_email='leonardbinet@gmail.com',
    license='MIT',
    url='https://github.com/leonardbinet/Transilien-Api',
    classifiers=[],
)
