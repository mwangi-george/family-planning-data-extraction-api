from contextvars import ContextVar


trace_id_ctx: ContextVar[str] = ContextVar("trace_id", default="-")