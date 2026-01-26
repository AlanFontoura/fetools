"""
FE Tools: general financial engineering tools for daily routines.
"""

from pathlib import Path

# Project root directory - all data paths are relative to this
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()

from .tools.po_sma_tools import (
    adjust_fco_table,
    add_accounts_to_fco,
    Account,
    Owner,
    Structure,
    Split,
)
from .tools.vnf import ValuesAndFlowsTools
from .tools.compliance_report import ComplianceReport
from .api.base_main import BaseMain

__all__ = [
    "PROJECT_ROOT",
    "adjust_fco_table",
    "add_accounts_to_fco",
    "Account",
    "Owner",
    "Structure",
    "Split",
    "ValuesAndFlowsTools",
    "ComplianceReport",
    "BaseMain",
]
