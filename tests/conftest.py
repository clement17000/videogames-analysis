"""
tests/conftest.py

Configuration pytest — ajoute le dossier dags/ au sys.path pour que les
modules dlt_pipelines.* et les DAGs soient importables sans installation.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))
