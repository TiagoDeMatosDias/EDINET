"""Job handlers: one module per job family, named by ``src.jobs.kinds``.

A handler is ``def handler(context: JobContext) -> dict``. It returns the result
that is sent with ``complete`` (JSON-serialisable, at most 32 MiB; larger output
goes in files). Files written directly into ``context.output_dir`` are uploaded
as artifacts, named by their file name, before the job completes.

Raise ``JobError(message, retryable=False)`` for an expected failure whose message
may be shown to users. Call ``context.checkpoint()`` regularly in long work: it
raises ``JobCancelledError`` once a cancel was requested. Any other exception
fails the job with a generic ``"<Kind> failed (<ExceptionType>)"`` message.

Handlers are resolved with ``src.jobs.kinds.handler_for``; nothing imports a
handler module until a job of its kind runs.
"""
