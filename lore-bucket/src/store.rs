// SPDX-FileCopyrightText: 2026 codercommand
// SPDX-License-Identifier: MIT

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use lore_base::lore_spawn;
use lore_base::types::FRAGMENT_SIZE_THRESHOLD;
use lore_storage::Address;
use lore_storage::AddressNotFound;
use lore_storage::Context;
use lore_storage::Fragment;
use lore_storage::FragmentFlags;
use lore_storage::Hash;
use lore_storage::ImmutableStore;
use lore_storage::Partition;
use lore_storage::PayloadNotFound;
use lore_storage::StoreError;
use lore_storage::StoreGetData;
use lore_storage::StoreMatch;
use lore_storage::StoreMatchResult;
use lore_storage::StoreObliterateStats;
use lore_storage::gc_event::GcEventSinkRef;
use lore_storage::immutable_store::CopyBehavior;
use zerocopy::FromBytes;
use zerocopy::IntoBytes;

use crate::client::ObjectClient;
use crate::client::ObjectError;
use crate::client::join_key;
use crate::snapshot;

const STRIPES: usize = 256;
const QUERY_BATCH: usize = 32;

/// Immutable durable store. Eviction is a no-op: this store is the composite
/// durable tier, and the cache cap belongs on the local tier.
pub struct BucketStore {
    client: Arc<dyn ObjectClient>,
    prefix: String,
    local_path: Option<PathBuf>,
    stripes: Vec<tokio::sync::Mutex<()>>,
    /// Held for one snapshot at a time. Holds the fingerprint of the last
    /// upload, so an unchanged cache directory is not sent again.
    snapshot_gate: tokio::sync::Mutex<Option<snapshot::Fingerprint>>,
}

impl BucketStore {
    /// `prefix` is already trimmed. `local_path` is the cache directory, the
    /// parent of `immutable/` and `mutable/`. The server passes it in.
    pub fn new(
        client: Arc<dyn ObjectClient>,
        prefix: impl Into<String>,
        local_path: Option<PathBuf>,
    ) -> Self {
        let mut stripes = Vec::with_capacity(STRIPES);
        for _ in 0..STRIPES {
            stripes.push(tokio::sync::Mutex::new(()));
        }
        Self {
            client,
            prefix: prefix.into(),
            local_path,
            stripes,
            snapshot_gate: tokio::sync::Mutex::new(None),
        }
    }

    /// Upload the index snapshot on a timer until this store is dropped.
    pub fn start_periodic(self: &Arc<Self>, every: Duration) {
        if every.is_zero() || self.local_path.is_none() {
            return;
        }
        let weak = Arc::downgrade(self);
        lore_spawn!(async move {
            loop {
                tokio::time::sleep(every).await;
                let Some(store) = weak.upgrade() else {
                    break;
                };
                if let Err(err) = store.snapshot().await {
                    tracing::warn!("bucket snapshot failed: {err}");
                }
            }
        });
    }

    async fn snapshot(&self) -> Result<(), snapshot::SnapshotError> {
        let Some(local) = &self.local_path else {
            return Ok(());
        };
        let mut last = self.snapshot_gate.lock().await;
        snapshot::upload_if_changed(self.client.as_ref(), &self.prefix, local, &mut last)
            .await
            .map(|_| ())
    }

    fn stripe(&self, hash: &Hash) -> &tokio::sync::Mutex<()> {
        &self.stripes[hash.data()[0] as usize]
    }

    fn payload_key(&self, hash: &Hash) -> String {
        join_key(
            &self.prefix,
            &format!("payloads/{}", hex::encode(hash.data())),
        )
    }

    fn meta_key(&self, hash: &Hash) -> String {
        join_key(&self.prefix, &format!("meta/{}", hex::encode(hash.data())))
    }

    fn marker_key(&self, hash: &Hash, partition: &Partition, context: &Context) -> String {
        join_key(
            &self.prefix,
            &format!(
                "assoc/{}/{}/{}",
                hex::encode(hash.data()),
                hex::encode(partition.data()),
                hex::encode(context.data())
            ),
        )
    }

    fn assoc_prefix(&self, hash: &Hash) -> String {
        join_key(
            &self.prefix,
            &format!("assoc/{}/", hex::encode(hash.data())),
        )
    }

    fn partition_prefix(&self, hash: &Hash, partition: &Partition) -> String {
        join_key(
            &self.prefix,
            &format!(
                "assoc/{}/{}/",
                hex::encode(hash.data()),
                hex::encode(partition.data())
            ),
        )
    }

    async fn resolve(
        &self,
        partition: Partition,
        address: Address,
    ) -> Result<StoreMatchResult, StoreError> {
        let marker = self.marker_key(&address.hash, &partition, &address.context);
        let payload = self.payload_key(&address.hash);
        let (marker_exists, payload_exists) =
            tokio::try_join!(self.head_required(&marker), self.head_required(&payload))?;
        if marker_exists && payload_exists {
            Ok(StoreMatchResult {
                match_made: StoreMatch::MatchFull,
                partition,
                context: address.context,
                stored_local: false,
                stored_durable: true,
            })
        } else {
            Ok(StoreMatchResult::default())
        }
    }

    async fn head_required(&self, key: &str) -> Result<bool, StoreError> {
        self.client.head(key).await.map_err(store_err)
    }

    async fn read_header(&self, hash: &Hash) -> Result<Fragment, StoreError> {
        let bytes = self.client.get(&self.meta_key(hash)).await.map_err(|err| {
            if err.is_not_found() {
                StoreError::from(PayloadNotFound::from(*hash))
            } else {
                store_err(err)
            }
        })?;
        let mut fragment = fragment_from_meta(&bytes)?;
        fragment.flags |= FragmentFlags::PayloadStoredDurable.bits();
        Ok(fragment)
    }
}

#[async_trait]
impl ImmutableStore for BucketStore {
    fn isolates_partitions(&self) -> bool {
        true
    }

    fn max_query_batch(&self) -> Option<usize> {
        Some(QUERY_BATCH)
    }

    async fn get(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        let marker = self.marker_key(&address.hash, &partition, &address.context);
        if !self.head_required(&marker).await? {
            return Err(StoreError::from(AddressNotFound::from(address)));
        }
        let fragment = self.read_header(&address.hash).await?;
        let payload = self
            .client
            .get(&self.payload_key(&address.hash))
            .await
            .map_err(|err| {
                if err.is_not_found() {
                    StoreError::from(PayloadNotFound::from(address.hash))
                } else {
                    store_err(err)
                }
            })?;
        if payload.len() != fragment.size_payload as usize {
            return Err(StoreError::internal(format!(
                "payload length {} does not match header {}",
                payload.len(),
                fragment.size_payload
            )));
        }
        // dragon review L2: check the content hash before composite caches it.
        verify_payload(&address, &fragment, &payload)?;
        Ok(StoreGetData {
            fragment,
            match_made: StoreMatch::MatchFull,
            partition,
            payload: Some(payload),
        })
    }

    async fn query(
        self: Arc<Self>,
        partition: Partition,
        addresses: &[Address],
        results: &mut [StoreMatchResult],
    ) -> Result<(), StoreError> {
        if addresses.len() != results.len() {
            return Err(StoreError::internal(
                "query results length does not match addresses",
            ));
        }
        let outcomes = futures::stream::iter(addresses.iter().copied().enumerate())
            .map(|(index, address)| {
                let store = Arc::clone(&self);
                async move {
                    store
                        .resolve(partition, address)
                        .await
                        .map(|matched| (index, matched))
                }
            })
            .buffer_unordered(16)
            .collect::<Vec<_>>()
            .await;
        for outcome in outcomes {
            let (index, matched) = outcome?;
            results[index] = matched;
        }
        Ok(())
    }

    async fn get_metadata(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        let marker = self.marker_key(&address.hash, &partition, &address.context);
        if !self.head_required(&marker).await? {
            return Err(StoreError::from(AddressNotFound::from(address)));
        }
        let fragment = self.read_header(&address.hash).await?;
        Ok(StoreGetData::metadata(
            fragment,
            StoreMatch::MatchFull,
            partition,
        ))
    }

    async fn put(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        fragment: Fragment,
        payload: Option<Bytes>,
        force: bool,
    ) -> Result<(), StoreError> {
        check_fragment(&fragment, payload.as_ref())?;
        let _guard = self.stripe(&address.hash).lock().await;
        let marker = self.marker_key(&address.hash, &partition, &address.context);
        let payload_key = self.payload_key(&address.hash);
        let meta_key = self.meta_key(&address.hash);

        if let Some(bytes) = payload {
            let marker_exists = self.head_required(&marker).await?;
            if marker_exists && !force {
                return Ok(());
            }
            self.client
                .put(&payload_key, bytes)
                .await
                .map_err(store_err)?;
            self.client
                .put(&meta_key, header_bytes(fragment))
                .await
                .map_err(store_err)?;
            self.client
                .put(&marker, Bytes::new())
                .await
                .map_err(store_err)?;
        } else if !self.head_required(&payload_key).await? {
            return Err(StoreError::from(PayloadNotFound::from(address.hash)));
        } else {
            self.client
                .put(&marker, Bytes::new())
                .await
                .map_err(store_err)?;
        }
        Ok(())
    }

    async fn obliterate(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        stats: Arc<StoreObliterateStats>,
    ) -> Result<(), StoreError> {
        let _guard = self.stripe(&address.hash).lock().await;
        let marker = self.marker_key(&address.hash, &partition, &address.context);
        if !self.head_required(&marker).await? {
            return Err(StoreError::from(AddressNotFound::from(address)));
        }
        self.client.delete(&marker).await.map_err(store_err)?;
        stats.num_fragments.fetch_add(1, Ordering::Relaxed);
        let remaining = self
            .client
            .list(&self.assoc_prefix(&address.hash))
            .await
            .map_err(store_err)?;
        if remaining.is_empty() {
            self.client
                .delete(&self.payload_key(&address.hash))
                .await
                .map_err(store_err)?;
            self.client
                .delete(&self.meta_key(&address.hash))
                .await
                .map_err(store_err)?;
            stats.num_payloads.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn evict(
        self: Arc<Self>,
        _max_capacity: usize,
        _sync_data: bool,
        _sink: Option<GcEventSinkRef>,
    ) -> Result<usize, StoreError> {
        Ok(0)
    }

    async fn compact(
        self: Arc<Self>,
        _max_size: usize,
        _at: Option<usize>,
        _sync_data: bool,
        _sink: Option<GcEventSinkRef>,
    ) -> Result<Option<usize>, StoreError> {
        Ok(None)
    }

    async fn compact_resume_at(self: Arc<Self>) -> Option<usize> {
        None
    }

    async fn flush(self: Arc<Self>, _sync_data: bool) -> Result<(), StoreError> {
        self.snapshot()
            .await
            .map_err(|err| StoreError::internal(err.to_string()))
    }

    async fn verify(self: Arc<Self>, _heal: bool) -> Result<(), StoreError> {
        Ok(())
    }

    async fn copy(
        self: Arc<Self>,
        source_partition: Partition,
        source_address: Address,
        destination_partition: Partition,
        destination_context: Context,
        _behavior: CopyBehavior,
    ) -> Result<(), StoreError> {
        let _guard = self.stripe(&source_address.hash).lock().await;
        let source_exists = if source_address.context.is_zero() {
            let keys = self
                .client
                .list(&self.partition_prefix(&source_address.hash, &source_partition))
                .await
                .map_err(store_err)?;
            !keys.is_empty()
        } else {
            self.head_required(&self.marker_key(
                &source_address.hash,
                &source_partition,
                &source_address.context,
            ))
            .await?
        };
        if !source_exists {
            return Err(StoreError::from(AddressNotFound::from(source_address)));
        }
        if !self
            .head_required(&self.payload_key(&source_address.hash))
            .await?
        {
            return Err(StoreError::from(PayloadNotFound::from(source_address.hash)));
        }
        let destination = self.marker_key(
            &source_address.hash,
            &destination_partition,
            &destination_context,
        );
        self.client
            .put(&destination, Bytes::new())
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

fn check_fragment(fragment: &Fragment, payload: Option<&Bytes>) -> Result<(), StoreError> {
    if fragment.size_payload == 0 {
        return Err(StoreError::internal("fragment size_payload is 0"));
    }
    if fragment.size_payload as usize > FRAGMENT_SIZE_THRESHOLD {
        return Err(StoreError::internal(format!(
            "fragment size_payload {} exceeds {FRAGMENT_SIZE_THRESHOLD}",
            fragment.size_payload
        )));
    }
    if let Some(payload) = payload
        && payload.len() != fragment.size_payload as usize
    {
        return Err(StoreError::internal(format!(
            "payload length {} does not match size_payload {}",
            payload.len(),
            fragment.size_payload
        )));
    }
    Ok(())
}

/// Check that the bucket bytes hash to the address.
///
/// Composite caches whatever the durable tier returns, and Lore's local `get`
/// checks only lengths, so a damaged bucket object would otherwise be cached
/// and served until something verifies it. A compressed payload is expanded
/// to hash its content, the same way the local store's `verify` does. A codec
/// this build cannot expand (Oodle) is passed through unchecked, as before.
/// An unknown or reserved compression bit is refused, never passed through.
fn verify_payload(
    address: &Address,
    fragment: &Fragment,
    payload: &Bytes,
) -> Result<(), StoreError> {
    // Lore's own hash_fragment: expands a compressed payload and also checks
    // size_content against the expanded length, as the local store does.
    let content_hash = match lore_storage::hash::hash_fragment(*fragment, payload) {
        Ok(hash) => hash,
        // Only a payload marked exactly Oodle2 passes unchecked. Any other
        // codec this build cannot expand (a reserved compression bit) is
        // refused: the header comes from the bucket, so a header with an
        // unknown bit set would otherwise skip the hash check entirely.
        Err(lore_storage::FragmentError::NotSupported(_))
            if fragment.flags & FragmentFlags::PayloadCompressed.bits()
                == FragmentFlags::PayloadCompressedOodle2.bits() =>
        {
            tracing::debug!(hash = %address.hash, "bucket payload codec not supported; hash not checked");
            return Ok(());
        }
        Err(err) => {
            tracing::warn!(hash = %address.hash, "bucket payload does not expand: {err}");
            return Err(StoreError::internal(format!(
                "bucket payload for {} failed its hash check: does not expand: {err}",
                address.hash
            )));
        }
    };
    if content_hash != address.hash {
        tracing::warn!(hash = %address.hash, got = %content_hash, "bucket payload failed its hash check");
        return Err(StoreError::internal(format!(
            "bucket payload for {} failed its hash check, got {content_hash}",
            address.hash
        )));
    }
    Ok(())
}

fn header_bytes(mut fragment: Fragment) -> Bytes {
    let clear =
        FragmentFlags::PayloadStoredLocal.bits() | FragmentFlags::PayloadStoredDurable.bits();
    fragment.flags &= !clear;
    Bytes::copy_from_slice(fragment.as_bytes())
}

fn fragment_from_meta(bytes: &[u8]) -> Result<Fragment, StoreError> {
    Fragment::read_from_bytes(bytes)
        .map_err(|err| StoreError::internal(format!("fragment header is the wrong size: {err:?}")))
}

fn store_err(err: ObjectError) -> StoreError {
    StoreError::internal(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ObjectClient;
    use crate::memory::MemoryClient;

    fn part(byte: u8) -> Partition {
        let mut partition = Partition::default();
        partition.data_mut()[0] = byte;
        partition
    }

    /// Four bytes of `hash_byte`: the content whose hash `addr(hash_byte, _)` names.
    fn content(hash_byte: u8) -> Bytes {
        Bytes::from(vec![hash_byte; 4])
    }

    fn addr(hash_byte: u8, context_byte: u8) -> Address {
        let hash = lore_storage::hash::hash_slice(&content(hash_byte));
        let mut context = Context::default();
        context.data_mut()[0] = context_byte;
        Address { hash, context }
    }

    fn fragment(len: u32) -> Fragment {
        Fragment {
            flags: FragmentFlags::PayloadStoredLocal.bits(),
            size_payload: len,
            size_content: len as u64,
        }
    }

    fn store(client: Arc<MemoryClient>) -> Arc<BucketStore> {
        Arc::new(BucketStore::new(client, "guilds/one", None))
    }

    #[tokio::test]
    async fn put_get_and_query_round_trip() {
        let client = MemoryClient::new();
        let store = store(client);
        let address = addr(1, 2);
        store
            .clone()
            .put(part(3), address, fragment(4), Some(content(1)), false)
            .await
            .unwrap();

        let got = store.clone().get(part(3), address).await.unwrap();
        assert_eq!(got.match_made, StoreMatch::MatchFull);
        assert_eq!(got.payload.unwrap(), content(1));
        assert_ne!(
            got.fragment.flags & FragmentFlags::PayloadStoredDurable.bits(),
            0
        );
        assert_eq!(
            got.fragment.flags & FragmentFlags::PayloadStoredLocal.bits(),
            0
        );

        let mut results = [StoreMatchResult::default()];
        store
            .clone()
            .query(part(3), &[address], &mut results)
            .await
            .unwrap();
        assert_eq!(results[0].match_made, StoreMatch::MatchFull);
        assert!(results[0].stored_durable);
        assert!(!results[0].stored_local);

        let mut missing = [StoreMatchResult::default()];
        store
            .query(part(9), &[address], &mut missing)
            .await
            .unwrap();
        assert_eq!(missing[0].match_made, StoreMatch::MatchNone);
    }

    #[tokio::test]
    async fn shared_hash_survives_until_the_last_marker_is_gone() {
        let client = MemoryClient::new();
        let store = store(Arc::clone(&client));
        let first = addr(4, 1);
        let second = addr(4, 2);
        store
            .clone()
            .put(part(1), first, fragment(4), Some(content(4)), false)
            .await
            .unwrap();
        store
            .clone()
            .put(part(2), second, fragment(4), None, false)
            .await
            .unwrap();

        let stats = Arc::new(StoreObliterateStats::default());
        store
            .clone()
            .obliterate(part(1), first, Arc::clone(&stats))
            .await
            .unwrap();
        assert_eq!(stats.num_payloads.load(Ordering::Relaxed), 0);
        assert_eq!(
            store
                .clone()
                .get(part(2), second)
                .await
                .unwrap()
                .payload
                .unwrap(),
            content(4)
        );

        store
            .clone()
            .obliterate(part(2), second, Arc::clone(&stats))
            .await
            .unwrap();
        assert_eq!(stats.num_payloads.load(Ordering::Relaxed), 1);
        assert!(store.clone().get(part(2), second).await.is_err());
        assert!(
            client
                .list("guilds/one/payloads/")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn put_without_bytes_fails_when_the_payload_is_absent() {
        let store = store(MemoryClient::new());
        let err = store
            .put(part(1), addr(5, 1), fragment(4), None, false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Payload not found"));
    }

    #[tokio::test]
    async fn evict_keeps_the_payload() {
        let store = store(MemoryClient::new());
        let address = addr(6, 1);
        store
            .clone()
            .put(part(1), address, fragment(4), Some(content(6)), false)
            .await
            .unwrap();
        assert_eq!(store.clone().evict(0, true, None).await.unwrap(), 0);
        assert!(store.get(part(1), address).await.is_ok());
    }

    #[tokio::test]
    async fn copy_adds_a_marker_without_a_second_payload() {
        let client = MemoryClient::new();
        let store = store(Arc::clone(&client));
        store
            .clone()
            .put(part(1), addr(7, 1), fragment(4), Some(content(7)), false)
            .await
            .unwrap();
        store
            .clone()
            .copy(
                part(1),
                Address {
                    hash: addr(7, 1).hash,
                    context: Context::default(),
                },
                part(2),
                addr(7, 3).context,
                CopyBehavior::default(),
            )
            .await
            .unwrap();
        assert!(store.get(part(2), addr(7, 3)).await.is_ok());
        assert_eq!(client.list("guilds/one/payloads/").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn flush_snapshots_the_index_and_skips_packs() {
        let client = MemoryClient::new();
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("immutable").join("index").join("aa");
        std::fs::create_dir_all(index.join("pack")).unwrap();
        std::fs::write(index.join("index_00"), b"idx").unwrap();
        std::fs::write(index.join("pack").join("data"), b"pack").unwrap();
        let store = Arc::new(BucketStore::new(
            client.clone(),
            "",
            Some(root.path().to_path_buf()),
        ));
        store.flush(true).await.unwrap();
        let keys = client.list("snapshot/").await.unwrap();
        assert!(
            keys.iter()
                .any(|key| key.ends_with("immutable/index/aa/index_00"))
        );
        assert!(keys.iter().all(|key| !key.contains("/pack/")));
    }

    #[tokio::test]
    async fn get_rejects_a_damaged_bucket_payload() {
        let client = MemoryClient::new();
        let store = store(Arc::clone(&client));
        let address = addr(8, 1);
        store
            .clone()
            .put(part(1), address, fragment(4), Some(content(8)), false)
            .await
            .unwrap();
        // Same length, one byte flipped: what bit rot in the bucket looks like.
        let key = format!("guilds/one/payloads/{}", hex::encode(address.hash.data()));
        client
            .put(&key, Bytes::from(vec![8, 8, 9, 8]))
            .await
            .unwrap();
        let err = store.get(part(1), address).await.unwrap_err();
        assert!(err.to_string().contains("hash check"), "{err}");
    }

    #[tokio::test]
    async fn get_refuses_a_header_with_an_unknown_codec_bit() {
        let client = MemoryClient::new();
        let store = store(Arc::clone(&client));
        let address = addr(9, 1);
        // A reserved compression bit (0b1_0000): no codec in Lore uses it.
        let mut header = fragment(4);
        header.flags |= 0b1_0000;
        store
            .clone()
            .put(part(1), address, header, Some(Bytes::from(vec![1, 2, 3, 4])), false)
            .await
            .unwrap();
        let err = store.get(part(1), address).await.unwrap_err();
        assert!(err.to_string().contains("hash check"), "{err}");
    }
}
