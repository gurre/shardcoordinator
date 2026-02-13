// Package main demonstrates Route53-based distributed leader election using shardcoordinator.
//
// This example shows how to use the Route53 locker backend to coordinate leadership
// across multiple processes. The leader continuously processes work while followers
// wait for leadership acquisition. Leadership transfers gracefully when the leader
// stops or crashes.
//
// Usage:
//
//	go run main.go \
//	  --hosted-zone-id Z1234567890ABC \
//	  --domain-name example.com \
//	  --shard-id my-shard
//
// Required AWS Permissions:
//   - route53:GetHostedZone
//   - route53:ListResourceRecordSets
//   - route53:ChangeResourceRecordSets
//
// The lock is stored as a TXT record in Route53. For example, with:
//   - record-prefix: lock
//   - shard-id: analytics
//   - domain-name: example.com
//
// The lock record will be: lock.analytics.example.com
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/google/uuid"
	"github.com/gurre/shardcoordinator"
	"github.com/gurre/shardcoordinator/route53lock"
)

// appConfig holds all command-line flags and runtime configuration.
type appConfig struct {
	hostedZoneID  string        // Route53 hosted zone ID where lock records will be created
	domainName    string        // Base domain name for constructing lock record names
	recordPrefix  string        // DNS prefix for lock records (default: "lock")
	shardID       string        // Logical shard identifier for the resource being coordinated
	leaseDuration time.Duration // How long a leadership claim remains valid without renewal
	renewPeriod   time.Duration // How frequently the leader renews its lease
	workInterval  time.Duration // How often to poll leadership status and simulate work
	verifyOnly    bool          // If true, verify AWS setup and exit without running coordination
}

// parseFlags processes command-line arguments and validates required configuration.
// Exits with error message if required flags are missing or invalid.
func parseFlags() appConfig {
	cfg := appConfig{}

	flag.StringVar(&cfg.hostedZoneID, "hosted-zone-id", "", "Route53 hosted zone ID (required)")
	flag.StringVar(&cfg.domainName, "domain-name", "", "Base domain name (required)")
	flag.StringVar(&cfg.recordPrefix, "record-prefix", "lock", "DNS prefix for lock records")
	flag.StringVar(&cfg.shardID, "shard-id", "demo-shard", "Logical shard identifier")
	flag.DurationVar(&cfg.leaseDuration, "lease-duration", 15*time.Second, "Leadership lease duration")
	flag.DurationVar(&cfg.renewPeriod, "renew-period", 5*time.Second, "Renewal attempt frequency")
	flag.DurationVar(&cfg.workInterval, "work-interval", 2*time.Second, "Simulated work interval")
	flag.BoolVar(&cfg.verifyOnly, "verify-only", false, "Verify setup and exit")

	flag.Parse()

	// Validate required flags
	if cfg.hostedZoneID == "" {
		log.Fatalf("--hosted-zone-id is required")
	}
	if cfg.domainName == "" {
		log.Fatalf("--domain-name is required")
	}
	if cfg.leaseDuration <= 0 {
		log.Fatalf("--lease-duration must be positive")
	}
	if cfg.renewPeriod <= 0 {
		log.Fatalf("--renew-period must be positive")
	}
	if cfg.workInterval <= 0 {
		log.Fatalf("--work-interval must be positive")
	}
	if cfg.recordPrefix == "" {
		log.Fatalf("--record-prefix cannot be empty")
	}
	if cfg.shardID == "" {
		log.Fatalf("--shard-id cannot be empty")
	}

	return cfg
}

// verifySetup confirms the Route53 hosted zone exists and IAM permissions are sufficient.
// This performs a preflight check to catch configuration errors before starting coordination.
// Exits with detailed error message if verification fails.
func verifySetup(ctx context.Context, cfg appConfig, r53Client *route53.Client) {
	log.Printf("[INFO] Verifying Route53 hosted zone: %s", cfg.hostedZoneID)

	// Verify hosted zone exists and is accessible
	getZoneInput := &route53.GetHostedZoneInput{
		Id: &cfg.hostedZoneID,
	}
	zoneOutput, err := r53Client.GetHostedZone(ctx, getZoneInput)
	if err != nil {
		log.Fatalf("Hosted zone %s not found or inaccessible: %v", cfg.hostedZoneID, err)
	}

	log.Printf("[INFO] Found hosted zone: %s", *zoneOutput.HostedZone.Name)

	// Verify IAM permissions by attempting to list records
	// This requires route53:ListResourceRecordSets permission
	listRecordsInput := &route53.ListResourceRecordSetsInput{
		HostedZoneId: &cfg.hostedZoneID,
		MaxItems:     int32Ptr(1),
	}
	_, err = r53Client.ListResourceRecordSets(ctx, listRecordsInput)
	if err != nil {
		log.Fatalf("IAM permissions insufficient. Required: route53:ListResourceRecordSets, route53:ChangeResourceRecordSets. Error: %v", err)
	}

	log.Printf("[INFO] IAM permissions verified")
}

// generateOwnerID creates a globally unique identifier for this process.
// The format is: hostname-pid-uuid, ensuring uniqueness even when multiple
// processes run on the same host. Characters that would violate ownerID
// validation rules (spaces, tabs, newlines, quotes) are replaced with dashes.
//
// See coordinator.go:217-230 for ownerID validation rules.
func generateOwnerID() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %v", err)
	}

	// Sanitize hostname to comply with ownerID validation rules
	// coordinator.go:224 rejects spaces, tabs, newlines, and quotes
	hostname = strings.ReplaceAll(hostname, " ", "-")
	hostname = strings.ReplaceAll(hostname, "\t", "-")
	hostname = strings.ReplaceAll(hostname, "\n", "-")
	hostname = strings.ReplaceAll(hostname, "\"", "-")

	pid := os.Getpid()
	uid := uuid.New().String()

	return fmt.Sprintf("%s-%d-%s", hostname, pid, uid)
}

// setupSignalHandler configures graceful shutdown on SIGTERM or SIGINT.
// Cancels the provided context when a shutdown signal is received, allowing
// the main loop to exit cleanly and the deferred coordinator.Stop() to execute.
func setupSignalHandler(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigChan
		log.Println("[INFO] Shutdown signal received")
		cancel()
	}()
}

// int32Ptr returns a pointer to the given int32 value.
// Helper function for AWS SDK calls that require pointer parameters.
func int32Ptr(v int32) *int32 {
	return &v
}

func main() {
	cfg := parseFlags()

	log.Println("[INFO] Starting Route53 Leader Election Example")

	// Create cancellable context for coordinating shutdown across goroutines
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load AWS configuration from environment, shared config file, or EC2 instance metadata
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	r53Client := route53.NewFromConfig(awsConfig)

	// Verify Route53 access and IAM permissions before starting coordination
	verifySetup(ctx, cfg, r53Client)

	if cfg.verifyOnly {
		log.Println("[INFO] Verification complete. Exiting.")
		return
	}

	// Generate globally unique owner ID to prevent duplicate ownerID split-brain
	// See README.md Failure Modes section, line 517-522 for duplicate ownerID risks
	ownerID := generateOwnerID()
	log.Printf("[INFO] Owner: %s", ownerID)
	log.Printf("[INFO] Shard: %s", cfg.shardID)

	lockRecordName := fmt.Sprintf("%s.%s.%s", cfg.recordPrefix, cfg.shardID, cfg.domainName)
	log.Printf("[INFO] Lock Record: %s", lockRecordName)

	// Create Route53 locker backend
	// Lock state is stored in a TXT record with format: "ownerID timestamp"
	lock, err := route53lock.New(route53lock.Config{
		Client:       r53Client,
		HostedZoneID: cfg.hostedZoneID,
		DomainName:   cfg.domainName,
		RecordPrefix: cfg.recordPrefix,
	})
	if err != nil {
		log.Fatalf("Failed to create Route53 lock: %v", err)
	}

	// Create coordinator for this shard
	// The coordinator will continuously compete for leadership and renew if acquired
	coordinator, err := shardcoordinator.New(shardcoordinator.Config{
		ShardID:       cfg.shardID,
		OwnerID:       ownerID,
		Locker:        lock,
		LeaseDuration: cfg.leaseDuration,
		RenewPeriod:   cfg.renewPeriod,
	})
	if err != nil {
		log.Fatalf("Failed to create coordinator: %v", err)
	}

	// Setup signal handler to enable graceful shutdown with Ctrl+C or SIGTERM
	setupSignalHandler(cancel)

	// Start the coordination loop in a background goroutine
	// This begins attempting to acquire leadership
	if err := coordinator.Start(ctx); err != nil {
		log.Fatalf("Failed to start coordinator: %v", err)
	}

	// Ensure coordinator.Stop() is called on exit to release lock gracefully
	// Use separate context with timeout to avoid hanging on slow Release()
	// See README.md line 524 for Stop() hanging risk during network partition
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := coordinator.Stop(stopCtx); err != nil {
			log.Printf("[ERROR] Failed to stop coordinator: %v", err)
		}
	}()

	// Track leadership transitions to detect acquire/lose events
	wasLeader := false
	workCounter := 0

	ticker := time.NewTicker(cfg.workInterval)
	defer ticker.Stop()

	// Main work loop: check leadership status and process work if leader
	for {
		select {
		case <-ctx.Done():
			// Context cancelled by signal handler or programmatic shutdown
			// Deferred coordinator.Stop() will execute after return
			log.Println("[INFO] Shutting down gracefully...")
			return

		case <-ticker.C:
			// Poll current leadership status
			// IsLeader() is thread-safe and returns coordinator's current role
			isLeader := coordinator.IsLeader()

			// Detect leadership transitions for logging
			if isLeader && !wasLeader {
				log.Println("[LEADER] Acquired leadership!")
			} else if !isLeader && wasLeader {
				log.Println("[FOLLOWER] Lost leadership")
				workCounter = 0 // Reset work counter on demotion
			}

			// Only the leader performs work
			// Followers wait passively for leadership acquisition
			if isLeader {
				workCounter++
				log.Printf("[LEADER] Processing work #%d", workCounter)
				// In real applications, this is where leader-only tasks execute:
				// - Processing queues
				// - Running scheduled jobs
				// - Coordinating distributed operations
			} else {
				log.Println("[FOLLOWER] Waiting for leadership...")
				// Followers can perform read-only operations or simply idle
			}

			wasLeader = isLeader
		}
	}
}
