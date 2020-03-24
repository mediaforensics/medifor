# Analytic Proxy

The analyticproxy binary manages file staging and unstaging for analytics. The primary purposes of the
proxy are to
- Keep the protocol simple for analytic developers,
- Keep file-fetching auth tokens private (in the proxy, not in the analytic),
- While allowing things like pulling from / pushing to S3 APIs.
