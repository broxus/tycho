use tycho_types::models::GenesisInfo;

/// Get genesis info and detect if it was updated since the previous mc block collation
pub fn choose_genesis_info(
    curr_genesis_info: GenesisInfo,
    prev_genesis_info: Option<GenesisInfo>,
    genesis_info_override: Option<GenesisInfo>,
) -> (GenesisInfo, bool) {
    if let Some(genesis_info_override) = &genesis_info_override
        && genesis_info_override.overrides(&curr_genesis_info)
    {
        // new genesis info provided from global config and overrides the current from the last mc block
        (*genesis_info_override, true)
    } else if prev_genesis_info.is_none_or(|prev| !curr_genesis_info.overrides(&prev)) {
        // genesis info was not updated in the last mc block
        // NOTE: When prev mc data not exists it may be a start from zerostate or persistent
        //      we cannot detect if genesis was really updated, so consider `genesis_updated = false`.
        //      But is does not matter, because it will be on the node start so cache/buffers will be empty anyway.
        (curr_genesis_info, false)
    } else {
        // genesis info from the last mc block overrides the previous mc block before it

        // NOTE: Special case when collating the next block after mc block
        //      that applied genesis override from the global config:
        //      Genesis will override the previous mc block before it
        //      but externals were already reset in the last mc block collation.
        //      So we should not reset again.
        // So we consider the current genesis info was updated only if it overrides info from the global config
        let genesis_updated_after_override = genesis_info_override
            .is_none_or(|gi_override| curr_genesis_info.overrides(&gi_override));

        (curr_genesis_info, genesis_updated_after_override)
    }
}

#[cfg(test)]
mod tests {
    use tycho_types::models::GenesisInfo;

    use super::choose_genesis_info;

    #[test]
    fn test_choose_genesis_info() {
        let g1 = GenesisInfo {
            start_round: 100,
            genesis_millis: 1_000,
        };
        let g2 = GenesisInfo {
            start_round: 200,
            genesis_millis: 2_000,
        };
        let g3 = GenesisInfo {
            start_round: 300,
            genesis_millis: 3_000,
        };

        struct Case {
            name: &'static str,
            curr: GenesisInfo,
            prev: Option<GenesisInfo>,
            override_: Option<GenesisInfo>,
            expected_genesis: GenesisInfo,
            expected_updated: bool,
        }

        let cases = [
            // Override has higher genesis than current: return override, updated = true.
            Case {
                name: "override_higher_prev_absent",
                curr: g1,
                prev: None,
                override_: Some(g3),
                expected_genesis: g3,
                expected_updated: true,
            },
            // Current does not override previous: not updated.
            Case {
                name: "prev_blocks_update_no_override",
                curr: g2,
                prev: Some(g2),
                override_: None,
                expected_genesis: g2,
                expected_updated: false,
            },
            // Current does not override previous even with lower/equal override:
            // previous branch has priority and reports "not updated".
            Case {
                name: "prev_blocks_update_with_lower_override",
                curr: g2,
                prev: Some(g2),
                override_: Some(g1),
                expected_genesis: g2,
                expected_updated: false,
            },
            // Current overrides previous and there is no override from config: updated.
            Case {
                name: "curr_overrides_prev_no_override",
                curr: g2,
                prev: Some(g1),
                override_: None,
                expected_genesis: g2,
                expected_updated: true,
            },
            // No previous data and no override: not updated.
            Case {
                name: "no_prev_no_override",
                curr: g1,
                prev: None,
                override_: None,
                expected_genesis: g1,
                expected_updated: false,
            },
            // With no previous genesis, lower override still goes to "not updated" branch.
            Case {
                name: "no_prev_lower_override",
                curr: g2,
                prev: None,
                override_: Some(g1),
                expected_genesis: g2,
                expected_updated: false,
            },
            // With no previous genesis, equal override still goes to "not updated" branch.
            Case {
                name: "no_prev_equal_override",
                curr: g2,
                prev: None,
                override_: Some(g2),
                expected_genesis: g2,
                expected_updated: false,
            },
            // Current equals config override after previous update: should not update again.
            Case {
                name: "curr_equals_override_prev_present",
                curr: g2,
                prev: Some(g1),
                override_: Some(g2),
                expected_genesis: g2,
                expected_updated: false,
            },
            // Current overrides previous and config override is lower: updated.
            Case {
                name: "curr_overrides_lower_override_prev_present",
                curr: g2,
                prev: Some(g1),
                override_: Some(g1),
                expected_genesis: g2,
                expected_updated: true,
            },
        ];

        for case in cases {
            let (genesis_info, genesis_updated) =
                choose_genesis_info(case.curr, case.prev, case.override_);
            assert_eq!(genesis_info, case.expected_genesis, "{}", case.name);
            assert_eq!(genesis_updated, case.expected_updated, "{}", case.name);
        }
    }
}
