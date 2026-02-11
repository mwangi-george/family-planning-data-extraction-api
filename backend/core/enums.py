from enum import Enum


class Program(str, Enum):
    """
    Enumeration of possible programs types
    """
    FP = "FP"
    MNCH = "MNCH"