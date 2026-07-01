"""custosctl — local & remote dev/test CLI for the Custos platform.

See ``design/components/custosctl/design.md`` (COMP-011, milestone 0.2).
This package is the scaffold delivered by DEVCLI-IMPL-001 (#952): the
Click application, the ``CUSTOS_*`` configuration model, the ``--target``
abstraction, and the ``doctor`` preflight. Lifecycle and API commands are
added by the sibling DEVCLI tasks.
"""

from __future__ import annotations

__version__ = "0.2.0"

__all__ = ["__version__"]
