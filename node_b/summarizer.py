"""Compatibility shim: the summarizer lives in ``common.summarizer``.

Node A builds stubs for keys it spills to local disk, so the code is shared.
"""

from common.summarizer import build_stub, make_stub_summary

__all__ = ["build_stub", "make_stub_summary"]
