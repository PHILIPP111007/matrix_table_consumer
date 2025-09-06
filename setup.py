import os
from setuptools import setup, Extension, find_packages
from setuptools.command.build_ext import build_ext
import subprocess
from Cython.Build import cythonize


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


ext_modules = [
    Extension("main.so", sources=[]),
    Extension(
        "matrix_table_consumer.functions_py.convert_rows_to_hail",
        sources=["matrix_table_consumer/functions_py/convert_rows_to_hail.py"],
        language="c",
    ),
]


setup(
    name="matrix_table_consumer",
    version="1.2.5",
    author="Philipp Roschin",
    author_email="r.phil@yandex.ru",
    description="MatrixTableConsumer, which performs operations on the Hail MatrixTable.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    ext_modules=cythonize(ext_modules),
    cmdclass={"build_ext": BuildGoExtension},
    data_files=[
        ("", ["matrix_table_consumer/main.so"]),
        ("", ["matrix_table_consumer/main.h"]),
    ],
    include_package_data=True,
    zip_safe=False,
    license="MIT",
    install_requires=[
        "Cython==3.1.3",
        "hail==0.2.135",
        "pyspark==3.5.6",
        "tqdm==4.67.1",
        "pytest==8.4.1",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.12",
)
