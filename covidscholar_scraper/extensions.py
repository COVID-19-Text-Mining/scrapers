import logging

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration


class SentryLogging(object):
    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()

        dsn = crawler.settings['SENTRY_DSN']
        if dsn:
            sentry_logging = LoggingIntegration(
                level=logging.WARNING,
                event_level=logging.ERROR,
            )
            sentry_sdk.init(
                dsn=dsn,
                integrations=[sentry_logging]
            )

        return ext
