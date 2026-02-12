# Route53 Leader Election Example

This example demonstrates Route53-based distributed leader election using the shardcoordinator library. Multiple instances compete for leadership of a logical shard, with only one instance becoming the leader at any time.

## Overview

The example shows:

- Route53 lock configuration and initialization
- Unique owner ID generation (hostname-pid-uuid pattern)
- Graceful shutdown with proper lock release
- Clear visibility into leader/follower state transitions
- AWS resource verification before starting coordination

## Prerequisites

1. **AWS Account** with Route53 access
2. **Route53 Hosted Zone** (existing or create new)
3. **IAM Permissions**:
   - `route53:GetHostedZone`
   - `route53:ListResourceRecordSets`
   - `route53:ChangeResourceRecordSets`
4. **AWS Credentials** configured (via environment, ~/.aws/credentials, or IAM role)

## Quick Start

### Step 1: Create Hosted Zone (if needed)

If you don't have a hosted zone, create one:

```bash
aws route53 create-hosted-zone \
    --name coordination.example.com \
    --caller-reference $(date +%s)
```

Note the `HostedZone.Id` from the output (e.g., `Z1234567890ABC`).

**Important**: Hosted zones cost $0.50/month. Remember to delete it when done testing.

### Step 2: Verify Setup

Verify your hosted zone and IAM permissions:

```bash
go run ./cmd/r53example \
    --hosted-zone-id Z1234567890ABC \
    --domain-name coordination.example.com \
    --verify-only
```

Expected output:
```
[INFO] Starting Route53 Leader Election Example
[INFO] Verifying Route53 hosted zone: Z1234567890ABC
[INFO] Found hosted zone: coordination.example.com.
[INFO] IAM permissions verified
[INFO] Verification complete. Exiting.
```

### Step 3: Run the Example

Start the leader election example:

```bash
go run ./cmd/r53example \
    --hosted-zone-id Z1234567890ABC \
    --domain-name coordination.example.com
```

Expected output:
```
[INFO] Starting Route53 Leader Election Example
[INFO] Verifying Route53 hosted zone: Z1234567890ABC
[INFO] Found hosted zone: coordination.example.com.
[INFO] IAM permissions verified
[INFO] Owner: macbook-12345-550e8400-e29b-41d4-a716-446655440000
[INFO] Shard: demo-shard
[INFO] Lock Record: lock.demo-shard.coordination.example.com
[FOLLOWER] Waiting for leadership...
[LEADER] Acquired leadership!
[LEADER] Processing work #1
[LEADER] Processing work #2
```

## Testing Leader Election

Test that only one instance becomes leader when multiple instances run:

```bash
# Terminal 1
go run ./cmd/r53example \
    --hosted-zone-id Z1234567890ABC \
    --domain-name coordination.example.com

# Terminal 2 (in a separate terminal)
go run ./cmd/r53example \
    --hosted-zone-id Z1234567890ABC \
    --domain-name coordination.example.com
```

Observe that only one instance shows `[LEADER]` output while the other shows `[FOLLOWER]`.

## Testing Failover

Test automatic failover when the leader stops:

1. Run two instances (as shown above)
2. Identify which instance is the leader
3. Kill the leader instance with `Ctrl+C`
4. Observe the follower becoming leader within ~15 seconds (the default lease duration)

Expected behavior:
- Leader receives signal and shuts down gracefully
- Follower detects leader absence and acquires leadership
- New leader begins processing work

## DNS Record Inspection

View the TXT record to see the current lock state:

```bash
dig +short TXT lock.demo-shard.coordination.example.com
```

Example output:
```
"macbook-12345-550e8400-e29b-41d4-a716-446655440000 1739284700"
```

The record contains:
- Owner ID (hostname-pid-uuid)
- Lease expiration timestamp (Unix epoch)

## Configuration Options

| Flag | Default | Description |
|------|---------|-------------|
| `--hosted-zone-id` | *(required)* | Route53 hosted zone ID |
| `--domain-name` | *(required)* | Base domain name for lock records |
| `--record-prefix` | `lock` | DNS prefix for lock records |
| `--shard-id` | `demo-shard` | Logical shard identifier |
| `--lease-duration` | `15s` | Leadership lease duration |
| `--renew-period` | `5s` | How often to renew leadership |
| `--work-interval` | `2s` | Simulated work interval |
| `--verify-only` | `false` | Verify setup and exit without running |

Example with custom configuration:

```bash
go run ./cmd/r53example \
    --hosted-zone-id Z1234567890ABC \
    --domain-name coordination.example.com \
    --shard-id prod-shard-1 \
    --lease-duration 30s \
    --renew-period 10s \
    --work-interval 5s
```

## IAM Policy

Required IAM permissions for the example:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "route53:ChangeResourceRecordSets",
                "route53:ListResourceRecordSets",
                "route53:GetHostedZone"
            ],
            "Resource": "arn:aws:route53:::hostedzone/Z1234567890ABC"
        }
    ]
}
```

Replace `Z1234567890ABC` with your actual hosted zone ID.

## Cleanup

### Automatic Cleanup

Lock records are automatically deleted when the example exits gracefully (Ctrl+C or SIGTERM). No manual cleanup is needed for DNS records.

### Delete Hosted Zone

If you created a test hosted zone, delete it to avoid ongoing charges:

```bash
# First, ensure all records except NS and SOA are deleted
aws route53 list-resource-record-sets --hosted-zone-id Z1234567890ABC

# Delete the hosted zone
aws route53 delete-hosted-zone --id Z1234567890ABC
```

## Troubleshooting

### "Hosted zone not found"

**Problem**: `Hosted zone Z1234567890ABC not found or inaccessible`

**Solutions**:
- Verify the hosted zone ID is correct: `aws route53 list-hosted-zones`
- Check you're using the correct AWS credentials/profile
- Ensure the hosted zone exists in your account

### "IAM permissions insufficient"

**Problem**: `IAM permissions insufficient. Required: route53:ListResourceRecordSets, route53:ChangeResourceRecordSets`

**Solutions**:
- Verify your IAM user/role has the required permissions (see IAM Policy section)
- Check the resource ARN in the policy matches your hosted zone ID
- Ensure AWS credentials are properly configured

### "Multiple leaders detected"

**Problem**: Both instances show `[LEADER]` output

**Solutions**:
- Check for clock skew between hosts (ensure NTP is synchronized)
- Verify both instances use the same `--shard-id` and `--domain-name`
- Confirm both instances use the same hosted zone ID
- Check for network partitions or Route53 API issues

### "Failed to load AWS config"

**Problem**: `Failed to load AWS config: no EC2 IMDS role found`

**Solutions**:
- Configure AWS credentials: `aws configure`
- Set environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- Use AWS profiles: `AWS_PROFILE=myprofile go run ./cmd/r53example ...`
- For EC2 instances, ensure an IAM role is attached

### Lock records not being cleaned up

**Problem**: TXT records remain after the example exits

**Solutions**:
- Ensure you're using `Ctrl+C` or `SIGTERM` for graceful shutdown (not `kill -9`)
- Check logs for errors during shutdown
- Manually delete stale records:
  ```bash
  # List records to find the lock record
  aws route53 list-resource-record-sets --hosted-zone-id Z1234567890ABC

  # Delete manually if needed (create a change batch JSON)
  aws route53 change-resource-record-sets --hosted-zone-id Z1234567890ABC --change-batch file://delete-record.json
  ```

## Architecture Notes

### Owner ID Format

The example generates unique owner IDs using the pattern: `hostname-pid-uuid`

- **hostname**: Machine hostname (for human readability)
- **pid**: Process ID (ensures uniqueness on same host)
- **uuid**: Random UUID (ensures global uniqueness)

Example: `macbook-12345-550e8400-e29b-41d4-a716-446655440000`

### DNS Record Structure

Lock records use this naming pattern: `{record-prefix}.{shard-id}.{domain-name}`

Example: `lock.demo-shard.coordination.example.com`

This allows multiple independent locks in the same hosted zone by varying the shard ID.

### Lease Timing

The relationship between `--lease-duration` and `--renew-period`:

- **lease-duration**: How long leadership is valid
- **renew-period**: How often to refresh the lease

Recommended: `renew-period ≤ lease-duration / 3`

Example:
- Lease duration: 15s
- Renew period: 5s
- Provides ~2 renewal attempts before lease expires

## Next Steps

- Integrate into your application by replacing the simulated work loop
- Adjust timing parameters for your use case (faster for low-latency, slower for cost optimization)
- Monitor Route53 API costs if running many coordinators
- Consider using DynamoDB lock for lower latency if all instances run in the same AWS region
