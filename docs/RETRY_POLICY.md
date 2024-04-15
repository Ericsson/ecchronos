# Retry Policy

The Retry Policy configures the connection retry behavior of ecChronos with Cassandra. The Timeout specifies the period of time ecChronos will wait for Cassandra to start per attempt. The retry policy sets the maximum number of connection attempts (maxAttempts), which by default is 5, specifying the time unit as seconds by deafult. Additionally, it determines the delay between each connection attempt (delay), multiplied by the current attempt count.

If the calculated delay exceeds the allowed maximum (maxDelay), the maximum will be used. The maxDelay limits the maximum time a connection attempt can wait to execute. If set to 0, it disables maxDelay, and the delay interval will be calculated based on the number of attempts and the default delay. These settings aim to optimize the efficiency of connection attempts, ensuring the system waits an appropriate amount of time and makes a reasonable number of attempts before giving up.

## Example on ecc.yml

```yaml
    ##
    ## Connection Timeout for a CQL attempt.
    ## Specify a time to wait for cassandra to come up.
    ## Connection is tried based on retry policy delay calculations. Each connection attempt will use the timeout to calculate CQL connection process delay.
    ##
    timeout:
      time: 6
      unit: minutes
    retryPolicy:
      ## Max number of attempts ecChronos will try to connect with Cassandra.
      maxAttempts: 5
      ## Delay use to wait between an attempt and another, this value will be multiplied by the current attempt count powered by two.
      ## If the current attempt is 4 and the default delay is 5 seconds, so ((4(attempt) x 2) x 5(default delay)) = 40 seconds.
      ## If the calculated delay is greater than maxDelay, maxDelay will be used instead of the calculated delay
      delay: 1
      ## Maximum delay before the next connection attempt is made.
      ## Setting it as 0 will disable maxDelay and the delay interval will
      ## be calculated based on the attempt count and the default delay.
      maxDelay: 5
      unit: minutes
```

With the configuration above, ecChronos will follow this procedure if it is not being able to connect with Cassandra.

```log
[main] WARN  c.e.b.c.e.a.DefaultNativeConnectionProvider - Unable to connect through CQL using localhost:9042, retrying.
[main] WARN  c.e.b.c.e.a.DefaultNativeConnectionProvider - Connection failed in attempt 1 of 5. Retrying in 120 seconds.
[main] WARN  c.e.b.c.e.a.DefaultNativeConnectionProvider - Unable to connect through CQL using localhost:9042, retrying.
[main] WARN  c.e.b.c.e.a.DefaultNativeConnectionProvider - Connection failed in attempt 2 of 5. Retrying in 240 seconds.
[main] WARN  c.e.b.c.e.a.DefaultNativeConnectionProvider - Unable to connect through CQL using localhost:9042, retrying.
[main] WARN  c.e.b.c.e.a.DefaultNativeConnectionProvider - Connection failed in attempt 3 of 5. Retrying in 300 seconds.
[main] WARN  c.e.b.c.e.a.DefaultNativeConnectionProvider - Unable to connect through CQL using localhost:9042, retrying.
[main] WARN  c.e.b.c.e.a.DefaultNativeConnectionProvider - Connection failed in attempt 4 of 5. Retrying in 300 seconds.
[main] WARN  c.e.b.c.e.a.DefaultNativeConnectionProvider - Unable to connect through CQL using localhost:9042, retrying.
[main] WARN  c.e.b.c.e.a.DefaultNativeConnectionProvider - Connection failed in attempt 5 of 5. Retrying in 300 seconds.
[main] ERROR  c.e.b.c.e.a..c.e.RetryPolicyException: Failed to establish connection after all retry attempts.
```
