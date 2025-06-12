# 在flink-python目录下创建setup.py文件
cat > setup.py << EOF
import os
from setuptools import setup, find_packages

setup(
    name='pyflink',
    version='1.7.0',
    description='Apache Flink Python API',
    packages=find_packages('src/main/python'),
    package_dir={'': 'src/main/python'},
    install_requires=[
        'py4j==0.10.7',  # Flink 1.7.0默认使用的py4j版本
    ],
    include_package_data=True,
    zip_safe=False,
)
EOF