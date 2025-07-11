#[derive(Default)]
pub struct PerPeerStats {
    pub was_leader: u32,
    pub was_not_leader: u32,
    pub skipped_rounds: u32,
    pub valid_points: u32,
    pub equivocated: u32,
    pub invalid_points: u32,
    pub ill_formed_points: u32,
    pub references_skipped: u32,
}

impl std::ops::AddAssign for PerPeerStats {
    fn add_assign(&mut self, rhs: Self) {
        self.was_leader += rhs.was_leader;
        self.was_not_leader += rhs.was_not_leader;
        self.skipped_rounds += rhs.skipped_rounds;
        self.valid_points += rhs.valid_points;
        self.equivocated += rhs.equivocated;
        self.invalid_points += rhs.invalid_points;
        self.ill_formed_points += rhs.ill_formed_points;
        self.references_skipped += rhs.references_skipped;
    }
}
