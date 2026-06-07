//! The property EC repair-at-rest rests on: reconstructing the object from any
//! `k` survivors and re-encoding it reproduces EVERY shard — data AND parity —
//! byte-for-byte identically to the originals (including partial-trailing-stripe
//! lengths). This is why repair needs no new EC primitive: gather survivors ->
//! `decode_object` -> `encode_object` -> persist the rebuilt shard.

use ozone_ec::stripe::{decode_object, encode_object, EncodedShards};
use ozone_ec::Profile;

fn shard(s: &EncodedShards, profile: Profile, idx: usize) -> &[u8] {
    if idx < profile.data {
        &s.data[idx]
    } else {
        &s.parity[idx - profile.data]
    }
}

#[test]
fn decode_then_reencode_reproduces_every_shard() {
    let profiles = [
        Profile { data: 3, parity: 2, chunk_size: 8 },
        Profile { data: 6, parity: 3, chunk_size: 8 },
        Profile { data: 10, parity: 4, chunk_size: 8 },
    ];
    for profile in profiles {
        let total = profile.data + profile.parity;
        // Lengths that include empty, sub-cell, exact-stripe, and partial-stripe
        // (the case block-group length correctness hinges on).
        let lengths = [
            0usize,
            5,
            profile.chunk_size,
            profile.stripe_size() - 1,
            profile.stripe_size(),
            profile.stripe_size() + 5,
            2 * profile.stripe_size() + 3,
            77,
        ];
        for len in lengths {
            let data: Vec<u8> = (0..len).map(|i| (i * 7 + 3) as u8).collect();
            let original = encode_object(profile, &data).unwrap();

            // Drop each slot in turn (data or parity), recover, re-encode.
            for dropped in 0..total {
                let views: Vec<Option<&[u8]>> = (0..total)
                    .map(|i| (i != dropped).then(|| shard(&original, profile, i)))
                    .collect();
                let recovered = decode_object(profile, len, &views).unwrap();
                assert_eq!(
                    recovered, data,
                    "decode mismatch k={} p={} len={len} dropped={dropped}",
                    profile.data, profile.parity
                );
                let reencoded = encode_object(profile, &recovered).unwrap();
                for i in 0..total {
                    assert_eq!(
                        shard(&reencoded, profile, i),
                        shard(&original, profile, i),
                        "shard {i} differs after decode->re-encode (k={} p={} len={len} dropped={dropped})",
                        profile.data, profile.parity
                    );
                }
            }
        }
    }
}
