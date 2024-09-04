use weedb::rocksdb::MergeOperands;

/// flags are stored in their order from left to right
/// and may be merged from `false` to `true` but not vice versa
#[derive(Default)]
pub struct PointFlags {
    // points are stored only after verified: points with sig or digest mismatch are never stored;
    // certified points are validated, and validation takes place only after verification;
    // locally created points (incl. genesis) are the only not validated before get stored;
    pub is_ill_formed: bool, // some well-formness is checked only on validate()
    pub is_validated: bool,  // need to distinguish not-yet-validated from invalid
    pub is_valid: bool,      // a point may be validated as invalid but certified afterward
    pub is_trusted: bool,    // locally decided as not equivocated (cannot change decision later)
    pub is_certified: bool,  // some points won't be marked because they are already validated
    pub is_committed: bool,  // payload only
    pub is_used: bool,       // either anchor proof or trigger
}

#[repr(u8)]
enum PointFlagsMasks {
    IllFormed = 0b_1 << 7,
    Validated = 0b_1 << 6,
    Valid = 0b_1 << 5,
    Trusted = 0b_1 << 4,
    Certified = 0b_1 << 3,
    Committed = 0b_1 << 2,
    Used = 0b_1 << 1,
}

impl PointFlags {
    const FLAGS_BYTES: usize = 1;
    const DEFAULT_FLAGS: [u8; Self::FLAGS_BYTES] = [0];

    pub fn encode(&self) -> [u8; Self::FLAGS_BYTES] {
        let mut result = 0;

        if self.is_ill_formed {
            result |= PointFlagsMasks::IllFormed as u8;
        }
        if self.is_validated {
            result |= PointFlagsMasks::Validated as u8;
        }
        if self.is_valid {
            result |= PointFlagsMasks::Valid as u8;
        }
        if self.is_trusted {
            result |= PointFlagsMasks::Trusted as u8;
        }
        if self.is_certified {
            result |= PointFlagsMasks::Certified as u8;
        }
        if self.is_committed {
            result |= PointFlagsMasks::Committed as u8;
        }
        if self.is_used {
            result |= PointFlagsMasks::Used as u8;
        }

        [result]
    }

    pub fn decode(stored: &[u8]) -> Self {
        assert_eq!(
            stored.len(),
            Self::FLAGS_BYTES,
            "unexpected amount of bytes for stored flags",
        );
        let stored = stored[0];
        PointFlags {
            is_ill_formed: (PointFlagsMasks::IllFormed as u8 & stored) > 0,
            is_validated: (PointFlagsMasks::Validated as u8 & stored) > 0,
            is_valid: (PointFlagsMasks::Valid as u8 & stored) > 0,
            is_trusted: (PointFlagsMasks::Trusted as u8 & stored) > 0,
            is_certified: (PointFlagsMasks::Certified as u8 & stored) > 0,
            is_committed: (PointFlagsMasks::Committed as u8 & stored) > 0,
            is_used: (PointFlagsMasks::Used as u8 & stored) > 0,
        }
    }

    pub(crate) fn merge(
        _key: &[u8],
        stored: Option<&[u8]>,
        new_flags_queue: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = Self::DEFAULT_FLAGS;
        if let Some(stored) = stored {
            assert_eq!(
                stored.len(),
                Self::FLAGS_BYTES,
                "unexpected amount of bytes for stored flags",
            );
            result.copy_from_slice(stored);
        }

        for new_flags in new_flags_queue {
            assert_eq!(
                new_flags.len(),
                Self::FLAGS_BYTES,
                "unexpected amount of bytes for new flags"
            );
            for i in 0..Self::FLAGS_BYTES {
                result[i] |= new_flags[i];
            }
        }

        Some(Vec::from(result))
    }
}
