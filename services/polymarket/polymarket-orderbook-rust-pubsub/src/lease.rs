//! Exclusive Redis lease for the authoritative v3 WebSocket publisher.
//!
//! Acquiring the lease increments a persistent fencing generation. Every
//! Redis Stream append also verifies the opaque lease token atomically, so a
//! paused process cannot resume publishing after its lease expires and a new
//! collector takes over.

use std::time::Duration;

use anyhow::{ensure, Context, Result};
use redis::aio::ConnectionManager;
use redis::Script;
use tokio::sync::watch;
use tracing::info;
use uuid::Uuid;

const ACQUIRE_SCRIPT: &str = r#"
if redis.call('EXISTS', KEYS[1]) ~= 0 then
    return nil
end
local generation = redis.call('INCR', KEYS[2])
local token = tostring(generation) .. ':' .. ARGV[1]
redis.call('SET', KEYS[1], token, 'PX', ARGV[2])
return {generation, token}
"#;

const RENEW_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
"#;

const RELEASE_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
return redis.call('DEL', KEYS[1])
"#;

#[derive(Debug, Clone)]
pub struct PublisherLeaseConfig {
    pub redis_url: String,
    pub lease_key: String,
    pub generation_key: String,
    pub ttl: Duration,
    pub renew_interval: Duration,
}

#[derive(Clone)]
pub struct PublisherLease {
    conn: ConnectionManager,
    lease_key: String,
    token: String,
    generation: u64,
    ttl: Duration,
    renew_interval: Duration,
}

impl PublisherLease {
    pub async fn acquire(cfg: PublisherLeaseConfig) -> Result<Self> {
        ensure!(!cfg.ttl.is_zero(), "publisher lease TTL must be positive");
        ensure!(
            !cfg.renew_interval.is_zero() && cfg.renew_interval < cfg.ttl,
            "publisher lease renewal interval must be positive and less than its TTL",
        );
        let client = redis::Client::open(cfg.redis_url.as_str())
            .with_context(|| format!("invalid REDIS_URL: {}", cfg.redis_url))?;
        let mut conn = ConnectionManager::new(client)
            .await
            .context("connect Redis for publisher lease")?;
        let instance_id = Uuid::new_v4().to_string();
        let acquired: Option<(u64, String)> = Script::new(ACQUIRE_SCRIPT)
            .key(&cfg.lease_key)
            .key(&cfg.generation_key)
            .arg(instance_id)
            .arg(duration_ms(cfg.ttl)?)
            .invoke_async(&mut conn)
            .await
            .context("acquire publisher lease")?;
        let Some((generation, token)) = acquired else {
            anyhow::bail!(
                "publisher lease {} is already held; refusing duplicate collector",
                cfg.lease_key,
            );
        };
        info!(
            lease_key = cfg.lease_key,
            generation,
            ttl_ms = cfg.ttl.as_millis() as u64,
            "acquired authoritative publisher lease",
        );
        Ok(Self {
            conn,
            lease_key: cfg.lease_key,
            token,
            generation,
            ttl: cfg.ttl,
            renew_interval: cfg.renew_interval,
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn key(&self) -> &str {
        &self.lease_key
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub async fn renew_until_shutdown(mut self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let ttl_ms = duration_ms(self.ttl)?;
        let mut interval = tokio::time::interval(self.renew_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow_and_update() {
                        return Ok(());
                    }
                }
                _ = interval.tick() => {
                    let renewed: i64 = Script::new(RENEW_SCRIPT)
                        .key(&self.lease_key)
                        .arg(&self.token)
                        .arg(ttl_ms)
                        .invoke_async(&mut self.conn)
                        .await
                        .context("renew publisher lease")?;
                    ensure!(renewed == 1, "authoritative publisher lease was lost");
                }
            }
        }
    }

    pub async fn release(&mut self) -> Result<bool> {
        let released: i64 = Script::new(RELEASE_SCRIPT)
            .key(&self.lease_key)
            .arg(&self.token)
            .invoke_async(&mut self.conn)
            .await
            .context("release publisher lease")?;
        Ok(released == 1)
    }
}

fn duration_ms(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_millis()).context("publisher lease duration exceeds u64 milliseconds")
}
