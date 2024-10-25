use std::collections::VecDeque;

use anyhow::{anyhow, Result};
use everscale_types::models::ShardIdent;

#[derive(Debug, Clone, PartialEq)]
pub enum SplitMergeAction {
    Add(ShardIdent),
    Split(ShardIdent),
}

enum CalcSplitMergeStep<'a> {
    CheckAction(
        ShardIdent,
        Option<Vec<&'a ShardIdent>>,
        Option<SplitMergeAction>,
    ),
    DoAction(Vec<&'a ShardIdent>, SplitMergeAction),
}

/// Calculate the list of split/merge actions that are needed
/// to move from the current shards set to a new
pub fn calc_split_merge_actions(
    from_current_shards: &[ShardIdent],
    to_new_shards: Vec<&ShardIdent>,
) -> Result<Vec<SplitMergeAction>> {
    let full_shard_id = ShardIdent::new_full(0);
    let mut planned_actions = VecDeque::new();
    if from_current_shards.is_empty() {
        planned_actions.push_back(CalcSplitMergeStep::CheckAction(full_shard_id, None, None));
    } else {
        planned_actions.extend(
            from_current_shards
                .iter()
                .map(|&sh| CalcSplitMergeStep::CheckAction(sh, None, None)),
        );
    }

    let mut result_actions = vec![];

    for new_shard_id in to_new_shards.iter() {
        if from_current_shards.is_empty() {
            result_actions.push(SplitMergeAction::Add(**new_shard_id));
        }
    }

    let mut rest_to_shards = to_new_shards;
    while let Some(next_planned_action) = planned_actions.pop_front() {
        match next_planned_action {
            CalcSplitMergeStep::CheckAction(from_shard_id, sub_to_shards_opt, action_opt) => {
                if let Some(mut sub_to_shards) = sub_to_shards_opt {
                    rest_to_shards = std::mem::take(&mut sub_to_shards);
                }
                let mut to_shards = std::mem::take(&mut rest_to_shards);
                let mut child_to_shards = vec![];
                for to_shard_id in to_shards.drain(..) {
                    if &from_shard_id == to_shard_id {
                        // do not need to split o merge
                        if let Some(ref action) = action_opt {
                            result_actions.push(action.clone());
                        }
                    } else if from_shard_id.is_ancestor_of(to_shard_id) {
                        // need to split
                        child_to_shards.push(to_shard_id);
                    } else if to_shard_id.is_ancestor_of(&from_shard_id) {
                        // need to merge
                    } else {
                        rest_to_shards.push(to_shard_id);
                    }
                }
                if !child_to_shards.is_empty() {
                    if let Some(ref action) = action_opt {
                        result_actions.push(action.clone());
                    }
                    planned_actions.push_back(CalcSplitMergeStep::DoAction(
                        child_to_shards,
                        SplitMergeAction::Split(from_shard_id),
                    ));
                }
            }
            CalcSplitMergeStep::DoAction(child_to_shards, action) => match action {
                SplitMergeAction::Split(from_shard_id) => {
                    let (l_shard, r_shard) = from_shard_id.split().ok_or_else(|| {
                        anyhow!(
                            "Unable to split shard {}, MAX_SPLIT_DEPTH ({}) reached",
                            from_shard_id,
                            ShardIdent::MAX_SPLIT_DEPTH
                        )
                    })?;
                    planned_actions.push_back(CalcSplitMergeStep::CheckAction(
                        l_shard,
                        Some(child_to_shards),
                        Some(action.clone()),
                    ));
                    planned_actions.push_back(CalcSplitMergeStep::CheckAction(
                        r_shard,
                        None,
                        Some(action),
                    ));
                }
                SplitMergeAction::Add(_) => {}
            },
        }
    }

    result_actions.dedup_by(|a, b| a == b);

    Ok(result_actions)
}

#[cfg(test)]
mod tests {
    use everscale_types::models::ShardIdent;

    use super::calc_split_merge_actions;

    #[test]
    fn test_calc_split_merge_actions() {
        let shard_80 = ShardIdent::new_full(0);

        // split on 4 shards
        let (shard_40, shard_c0) = shard_80.split().unwrap();
        let (shard_20, shard_60) = shard_40.split().unwrap();
        let (shard_a0, shard_e0) = shard_c0.split().unwrap();

        println!("full shard {}", shard_80);
        println!("shard split 1 {}", shard_40);
        println!("shard split 1 {}", shard_c0);
        println!("shard split 2 {}", shard_20);
        println!("shard split 2 {}", shard_60);
        println!("shard split 2 {}", shard_a0);
        println!("shard split 2 {}", shard_e0);

        let shards_1_r = vec![&shard_80];
        let shards_1_l = &[shard_80];
        let actions = calc_split_merge_actions(&[], shards_1_r.clone()).unwrap();
        println!("split/merge actions from [] to [1]: {:?}", actions);

        let shards_4_r = vec![&shard_20, &shard_60, &shard_a0, &shard_e0];
        let actions = calc_split_merge_actions(&[], shards_4_r.clone()).unwrap();
        println!("split/merge actions from [] to [4]: {:?}", actions);

        let actions = calc_split_merge_actions(shards_1_l, shards_4_r.clone()).unwrap();
        println!("split/merge actions from [1] to [4]: {:?}", actions);

        let shards_2_l = &[shard_40, shard_c0];
        let actions = calc_split_merge_actions(shards_2_l, shards_4_r.clone()).unwrap();
        println!("split/merge actions from [2] to [4]: {:?}", actions);

        let shards_3_r = vec![&shard_40, &shard_a0, &shard_e0];
        let shards_3_l = &[shard_40, shard_a0, shard_e0];
        let actions = calc_split_merge_actions(shards_2_l, shards_3_r.clone()).unwrap();
        println!("split/merge actions from [2] to [3]: {:?}", actions);

        let actions = calc_split_merge_actions(shards_3_l, shards_4_r.clone()).unwrap();
        println!("split/merge actions from [3] to [4]: {:?}", actions);
    }
}
