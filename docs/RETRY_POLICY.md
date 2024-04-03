# Retry Policy

The Retry Policy configures the connection retry behavior of ecChronos with Cassandra. The Timeout specifies the period of time ecChronos will wait for Cassandra to start per attempt. The retry policy sets the maximum number of connection attempts (maxAttempts), which by default is 5, specifying the time unit as seconds by deafult. Additionally, it determines the delay between each connection attempt (delay), multiplied by the current attempt count.

If the calculated delay exceeds the allowed maximum (maxDelay), the maximum will be used. The maxDelay limits the maximum time a connection attempt can wait to execute. If set to 0, it disables maxDelay, and the delay interval will be calculated based on the number of attempts and the default delay. These settings aim to optimize the efficiency of connection attempts, ensuring the system waits an appropriate amount of time and makes a reasonable number of attempts before giving up.

## Example on ecc.yml

```yaml
    ##
    ## Connection Timeout for an CQL attempt.
    ## Specify a time to wait for cassandra to come up.
    ## Connection is tried based on retry policy delay calculations. Each connection attempt will use timeout to calculate CQL Connection proccess delay.
    ##
    timeout:
      time: 6
      unit: minutes
    retryPolicy:
      ## Max number of attempts that ecChronos will try to connect with Cassandra.
      maxAttempts: 10
      unit: minutes
      ## Delay use to wait between an attempt and another, this value will be multiplied by the current attempt count.
      ## If the calculated delay is greater than maxDelay, maxDelay will be used instead of the calculated delay
      delay: 1
      ## The max delay that one attempt can wait until run.
      ## Setting it as 0 will disable maxDelay and the delay interval will
      ## be calculated based on the attempt count and the default delay.
      maxDelay: 5
```
