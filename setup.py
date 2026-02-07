from pathlib import Path

from setuptools import setup, find_packages


ROOT = Path(__file__).resolve().parent


def _read_text(filename: str) -> str:
    return (ROOT / filename).read_text(encoding="utf-8")


def _read_requirements(filename: str) -> list[str]:
    lines = []
    for raw in _read_text(filename).splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        lines.append(line)
    return lines


setup(
    name='megaton',
    version='0.8.3',
    author='Makoto Shimizu',
    author_email='aa.analyst.ga@gmail.com',
    description='Utilities for Google Analytics, Google Analytics 4, Google Sheets, Search Console and Google Cloud Platform.',
    long_description=_read_text("README.md"),
    long_description_content_type='text/markdown',
    packages=find_packages(),
    install_requires=_read_requirements("requirements.txt"),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Internet",
    ],
    python_requires='>=3.11',
)
