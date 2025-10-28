use super::*;
use crate::index::forward::small::SMALL_CAP;

fn cid(v: u32) -> ClientId {
    ClientId::from(v)
}

#[test]
fn test() {
    let mut set = ForwardSet::new();
    let cap = SMALL_CAP as u32;

    // small set
    for i in 1..=cap {
        let _ = set.add_with_delta(cid(i));
        let _ = set.add_with_delta(cid(i)); // Duplicate check
    }
    assert!(set.is_small(), "Should be small at cap");
    assert_eq!(set.iter_vec().len(), cap as usize);

    // promotion
    let c = set.add_with_delta(cid(999));
    assert_eq!(c, 140); // on heap now
    assert!(!set.is_small(), "Should have promoted");

    // deletions
    assert!(set.remove(cid(999)));
    assert!(set.remove(cid(1)));
    assert!(!set.remove(cid(888)));

    let mut final_ids = set.iter_vec();
    final_ids.sort();
    assert_eq!(final_ids.len(), cap as usize - 1);
    assert!(!final_ids.contains(&cid(999)));
    assert!(!final_ids.contains(&cid(1)));
}

#[test]
fn test_is_empty_is_actually_accurate() {
    let mut set = ForwardSet::new();
    assert!(set.is_empty());
    let _ = set.add_with_delta(cid(1));
    assert!(!set.is_empty());
    set.remove(cid(1));
    assert!(set.is_empty(), "Empty check failed after removal");
}

#[test]
fn size_estimation() {
    let mut set = ForwardSet::new();
    assert_eq!(set.heap_bytes(), 0, "Small set should be 0");

    // Force promotion
    for i in 0..=SMALL_CAP as u32 {
        let _ = set.add_with_delta(cid(i));
    }

    let size = set.heap_bytes();
    assert!(size > 0, "Naive set must report heap size");

    if let ForwardSet::Naive(n) = set {
        assert_eq!(size, n.capacity() * (size_of::<ClientId>() + 1));
    } else {
        panic!()
    }
}
