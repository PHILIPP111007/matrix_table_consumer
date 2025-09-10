import os
import sys
from setuptools import setup, Extension, find_packages
from setuptools.command.build_ext import build_ext
import subprocess
import platform

from Cython.Build import cythonize
import numpy as np


with open("README.md", "r", encoding="utf-8") as file:
    long_description = file.read()


class BuildGoExtension(build_ext):
    """Class for compiling Go code."""

    def run(self):
        super().run()

        go_dir = os.path.join(os.getcwd(), "matrix_table_consumer")
        os.chdir(go_dir)
        env = os.environ.copy()
        env["CGO_ENABLED"] = "1"

        try:
            subprocess.check_call(
                [
                    "go",
                    "build",
                    "-o",
                    "main.so",
                    "-buildmode=c-shared",
                    "main.go",
                ],
                env=env,
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Error building Go module: {e}")
        finally:
            os.chdir("..")


system = platform.system()
if system == "Linux":
    compile_args = ["-O3", "-ffast-math", "-march=native", "-fno-wrapv"]
    os.environ["CC"] = "gcc"
elif system == "Darwin":  # macOS
    compile_args = ["-O3", "-ffast-math", "-fno-wrapv"]
    os.environ["CC"] = "clang"
elif system == "Windows":
    if os.environ.get("CC", "").endswith("gcc"):
        compile_args = ["-O3"]
    else:
        compile_args = ["/O2"]
else:
    sys.exit(1)


common_macros = [('NPY_NO_DEPRECATED_API', 'NPY_1_7_API_VERSION')]

ext_modules = [
    Extension(
        name="matrix_table_consumer.functions_py.convert_rows_to_hail",
        sources=["matrix_table_consumer/functions_py/convert_rows_to_hail.py"],
        language="c",
        extra_compile_args=compile_args,
        define_macros=common_macros,
    ),
    Extension(
        name="matrix_table_consumer.functions_py.sample_qc_analysis", 
        sources=["matrix_table_consumer/functions_py/sample_qc_analysis.py"],
        language="c",
        extra_compile_args=compile_args,
        include_dirs=[np.get_include()],
        define_macros=common_macros,
    ),
]

setup(
    name="matrix_table_consumer",
    version="1.2.8",
    author="Philipp Roschin",
    author_email="r.phil@yandex.ru",
    description="MatrixTableConsumer, which performs operations on the Hail MatrixTable.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    ext_modules=cythonize(
        ext_modules,
        annotate=False,
        verbose=True,
    ),
    cmdclass={"build_ext": BuildGoExtension},
    data_files=[
        ("", ["matrix_table_consumer/main.so"]),
        ("", ["matrix_table_consumer/main.h"]),
        ("", ["matrix_table_consumer/functions_py/sample_qc_analysis.c"]),
    ],
    entry_points={
        "console_scripts": [
            "vcf_tools=matrix_table_consumer.vcf_tools:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    license="MIT",
    install_requires=[
        "Cython==3.1.3",
        "hail==0.2.135",
        "pyspark==3.5.6",
        "tqdm==4.67.1",
        "pytest==8.4.1",
        "bio2zarr[vcf]==0.1.6",
        "zarr==2.18.7",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.12",
)
