"""Bulk insert strategies for Apache AGE."""

from src.strategies.protocol import BulkInsertStrategy
from src.strategies.s0_individual_merge import IndividualMergeStrategy
from src.strategies.s1_unwind_merge import UnwindMergeStrategy
from src.strategies.s2_copy_unwind import CopyUnwindStrategy
from src.strategies.s3_direct_sql import DirectSqlStrategy

__all__ = [
    "BulkInsertStrategy",
    "IndividualMergeStrategy",
    "UnwindMergeStrategy",
    "CopyUnwindStrategy",
    "DirectSqlStrategy",
]

# Ordered list of all strategies (slowest to fastest)
ALL_STRATEGIES = [
    IndividualMergeStrategy,
    UnwindMergeStrategy,
    CopyUnwindStrategy,
    DirectSqlStrategy,
]
