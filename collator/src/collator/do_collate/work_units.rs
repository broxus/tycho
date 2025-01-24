use std::time::Duration;

use everscale_types::models::WorkUnitsParamsPrepare;

use crate::collator::messages_reader::MessagesReaderMetrics;
use crate::tracing_targets;

pub struct PrepareMsgGroupsWu {
    pub total_wu: u64,
    pub total_elapsed: Duration,

    pub read_ext_msgs_wu: u64,
    pub read_ext_msgs_elapsed: Duration,

    pub read_existing_int_msgs_wu: u64,
    pub read_existing_int_msgs_elapsed: Duration,

    pub read_new_int_msgs_wu: u64,
    pub read_new_int_msgs_elapsed: Duration,

    pub add_msgs_to_groups_wu: u64,
    pub add_msgs_to_groups_elapsed: Duration,
}

impl PrepareMsgGroupsWu {
    pub fn calculate(
        wu_params_prepare: &WorkUnitsParamsPrepare,
        msgs_reader_metrics: &MessagesReaderMetrics,
        prepare_msg_groups_total_elapsed: Duration,
    ) -> Self {
        let &WorkUnitsParamsPrepare {
            fixed_part,
            read_ext_msgs,
            read_int_msgs,
            read_new_msgs,
            add_to_msg_groups,
            ..
        } = wu_params_prepare;

        let read_ext_msgs_wu = msgs_reader_metrics
            .read_ext_msgs_count
            .saturating_mul(read_ext_msgs as u64);
        let read_existing_int_msgs_wu = msgs_reader_metrics
            .read_int_msgs_from_iterator_count
            .saturating_mul(read_int_msgs as u64);
        let read_new_int_msgs_wu = msgs_reader_metrics
            .read_new_msgs_count
            .saturating_mul(read_new_msgs as u64);

        let add_msgs_to_groups_wu = msgs_reader_metrics
            .add_to_msgs_groups_ops_count
            .saturating_mul(add_to_msg_groups as u64);

        let total_wu = (fixed_part as u64)
            .saturating_add(read_ext_msgs_wu)
            .saturating_add(read_existing_int_msgs_wu)
            .saturating_add(read_new_int_msgs_wu)
            .saturating_add(add_msgs_to_groups_wu);

        let res = Self {
            total_wu,
            total_elapsed: prepare_msg_groups_total_elapsed,

            read_ext_msgs_wu,
            read_ext_msgs_elapsed: msgs_reader_metrics.read_ext_messages_timer.total_elapsed,

            read_existing_int_msgs_wu,
            read_existing_int_msgs_elapsed: msgs_reader_metrics
                .read_existing_messages_timer
                .total_elapsed,

            read_new_int_msgs_wu,
            read_new_int_msgs_elapsed: msgs_reader_metrics.read_new_messages_timer.total_elapsed,

            add_msgs_to_groups_wu,
            add_msgs_to_groups_elapsed: msgs_reader_metrics
                .add_to_message_groups_timer
                .total_elapsed,
        };

        tracing::debug!(target: tracing_targets::COLLATOR,
            "prepare_msg_groups_wu: total: (wu={}, elapsed={}, price={}), \
            read_ext_msgs: (count={}, param={}, wu={}, elapsed={}, price={}), \
            read_existing_int_msgs: (count={}, param={}, wu={}, elapsed={}, price={}), \
            read_new_int_msgs: (count={}, param={}, wu={}, elapsed={}, price={}), \
            add_to_msgs_groups_ops: (count={}, param={}, wu={}, elapsed={}, price={})",
            res.total_wu, res.total_elapsed.as_nanos(), res.total_wu_price(),
            msgs_reader_metrics.read_ext_msgs_count, read_ext_msgs,
            res.read_ext_msgs_wu, res.read_ext_msgs_elapsed.as_nanos(), res.read_ext_msgs_wu_price(),
            msgs_reader_metrics.read_int_msgs_from_iterator_count, read_int_msgs,
            res.read_existing_int_msgs_wu, res.read_existing_int_msgs_elapsed.as_nanos(), res.read_existing_int_msgs_wu_price(),
            msgs_reader_metrics.read_new_msgs_count, read_new_msgs,
            res.read_new_int_msgs_wu, res.read_new_int_msgs_elapsed.as_nanos(), res.read_new_int_msgs_wu_price(),
            msgs_reader_metrics.add_to_msgs_groups_ops_count, add_to_msg_groups,
            res.add_msgs_to_groups_wu, res.add_msgs_to_groups_elapsed.as_nanos(), res.add_msgs_to_groups_wu_price(),
        );

        res
    }

    pub fn total_wu_price(&self) -> f64 {
        if self.total_wu == 0 {
            return 0.0;
        }
        self.total_elapsed.as_nanos() as f64 / self.total_wu as f64
    }
    pub fn read_ext_msgs_wu_price(&self) -> f64 {
        if self.read_ext_msgs_wu == 0 {
            return 0.0;
        }
        self.read_ext_msgs_elapsed.as_nanos() as f64 / self.read_ext_msgs_wu as f64
    }
    pub fn read_existing_int_msgs_wu_price(&self) -> f64 {
        if self.read_existing_int_msgs_wu == 0 {
            return 0.0;
        }
        self.read_existing_int_msgs_elapsed.as_nanos() as f64
            / self.read_existing_int_msgs_wu as f64
    }
    pub fn read_new_int_msgs_wu_price(&self) -> f64 {
        if self.read_new_int_msgs_wu == 0 {
            return 0.0;
        }
        self.read_new_int_msgs_elapsed.as_nanos() as f64 / self.read_new_int_msgs_wu as f64
    }
    pub fn add_msgs_to_groups_wu_price(&self) -> f64 {
        if self.add_msgs_to_groups_wu == 0 {
            return 0.0;
        }
        self.add_msgs_to_groups_elapsed.as_nanos() as f64 / self.add_msgs_to_groups_wu as f64
    }
}
