# Agents on Aurora import this package, so the exchange fix is in place before they listen
from intermediate.custom_agents import exchange_retry

exchange_retry.install()
