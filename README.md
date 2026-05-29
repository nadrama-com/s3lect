# S3lect

S3lect is a leader election package for Go which uses S3 / object storage as the coordination mechanism. Any object storage provider which supports conditional writes (e.g. If-Match) can be used.

It is designed for cloud-native applications that require reliable leader election with minimal operational overhead and cost-effective scaling.

S3lect uses the consistency guarantees and atomic operations of object storage to provide safe leader election across multiple instances, with configurable polling intervals and an optional peer-polling setting for object storage read/cost reduction.

S3lect is a subproject of [Podplane](https://podplane.dev), the Open Source Kubernetes distribution & PaaS. S3lect was created by [Nadrama](https://nadrama.com) for use in [Netsy](https://netsy.dev) and [Nstance](https://nstance.dev).

## Key Features

- **Object-storage-based coordination**: No additional infrastructure required beyond S3/object storage
- **Dual-interval optimization**: Frequent polling during transitions, infrequent or peer-polling during stable periods
- **Peer communication mode**: Optional HTTP-based leader health checks to minimize storage reads during polling
- **Configurable timeouts**: Tunable leader detection and failover timing
- **Simple retry logic**: Built-in resilience to transient failures such as networking errors

## Leader Election Algorithm

### Core Algorithm

1. **Read leader lockfile** from storage at the configured lockfile key/path

2. **Followers evaluate leader status**:
   - If no lockfile exists → attempt to become leader, using empty ETag
   - If leader hasn't updated within timeout period → attempt to become leader, using last known ETag

3. **Attempting to become leader**:
   - Write new leader record with conditional object storage operations (using ETag from point 2 above), exit attempt on failure
   - Ensure minimum/grace timeout period size gap since becoming leader to prevent "split-brain" scenarios

4. **Remaining a leader**: 
   - Leaders continuously update their timestamp in object storage every (configurable) interval
   - Failed updates (after configurable retries/timeout) result in automatic leadership resignation

### Dual-Interval System

S3lect operates in two intervals, and the latter interval can use two different modes to balance cost and performance:

**Frequent Interval (default: 5 seconds)**
- Used during leadership transitions and instability
- All instances poll object storage directly every X seconds for leader status
- Ensures fast failover detection (i.e. 11-15 seconds typical)
- Automatically engaged when peer communication fails

**Infrequent Interval (default: 30 seconds)**  
- Used during stable periods with established leadership
- Reduces object storage operations significantly
- Two sub-modes available:
  - **Object Storage Mode**: Standard object storage polling at reduced frequency: all instances poll storage directly every Y seconds for leadership status
  - **Peer Mode**: Followers check leader health via leader's HTTP API, and fallback to polling storage on failure/timeout

### Object Storage File Format

The object storage file format is a JSON document with the following fields:

- `leaderID`: Unique identifier for the current leader instance
- `leaderAddr`: Network address of the current leader (for peer communication)
- `lastUpdated`: Timestamp of the last update to the leader record

e.g.

```json
{
  "leaderID": "server-001",
  "leaderAddr": "10.0.1.42:8443",
  "lastUpdated": "2024-10-27T10:30:45Z"
}
```

### Peer Communication Protocol

When peer mode is enabled, followers in infrequent interval will:

1. **Attempt peer health check**: HTTPS GET to `https://{leaderAddr}{peerHealthPath}` using cached leader address (default peer health path: `/health/leadership`)
2. **On success**: Use the leader data from peer response, skip storage read entirely, continue infrequent polling  
3. **On failure**: Fall back to storage read to get current leader info, switch to frequent interval with direct storage polling

The peer health endpoint returns the same JSON document as stored in object storage (as described in the previous section above).

* We have opted to use a JSON HTTP API instead of gRPC or alternative for simplicity and parity with the object storage lockfile format

### Resilience and Retry Logic

All network operations (object storage and peer communication) include automatic retry:

1. **Immediate attempt**
2. **100ms delayed retry** on failure
3. **1-second delayed retry** on second failure
4. **Give up** and continue with election logic

This provides resilience against transient network issues while maintaining responsive failover timing.

## Configuration

S3lect is configured through the `ElectorConfig` structure:

- **LockfilePath**: object storage key/path for the leader lockfile (e.g., "leader/my-group.json")
- **ServerID**: Unique identifier for this instance
- **ServerAddr**: Network address for peer communication (e.g., "10.0.1.42:8443")
- **FrequentInterval**: Polling interval during transitions (default: 5s)
- **InfrequentInterval**: Polling interval during stable periods (default: 30s)  
- **LeaderTimeout**: Time before considering leader failed (default: 15s)
- **PeerMode**: Enable HTTP-based leader health checks (default: false)
- **PeerHealthPath**: HTTP path for the peer leader health check endpoint (default: "/health/leadership")
- **PeerTimeout**: Timeout for peer health check requests (default: 3s)

## Integration Requirements

### Storage Interface

S3lect requires a storage implementation providing:
- `Get(ctx, key)` - Read object and return it with its ETag
- `PutIfMatch(ctx, key, data, etag)` - Conditional put operation (using ETag from `Get`)

S3lect accepts the storage implementation as a parameter in the `ElectorConfig` structure,
and if not specified it falls back to the S3 implementation in the AWS SDK for Go v2.

### Leadership Callbacks

Applications can register callbacks to receive leadership change notifications, enabling immediate response to election events.

## Operational Characteristics

- **Failover time**: 11-15 seconds typical, up to 30 seconds worst-case
- **Scalability**: Follower count doesn't significantly impact leader object storage operations in peer mode
- **Dependencies**: Only requires object storage and standard HTTP client

## License

S3lect is licensed under the Apache License, Version 2.0.
Copyright The Podplane Authors.
See the [LICENSE](./LICENSE) file for details.
