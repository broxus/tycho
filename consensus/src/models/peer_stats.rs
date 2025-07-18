use crate::models::Round;

#[derive(Debug)]
pub struct PeerStats {
    pub first_round: u32,
    pub last_round: u32,
    pub was_leader: u32,
    pub was_not_leader: u32,
    pub skipped_rounds: u32,
    pub valid_points: u32,
    pub equivocated: u32,
    pub invalid_points: u32,
    pub ill_formed_points: u32,
    pub references_skipped: u32,
}

impl PeerStats {
    pub fn new(round: Round) -> Self {
        Self {
            first_round: round.0,
            last_round: round.0,
            was_leader: 0,
            was_not_leader: 0,
            skipped_rounds: 0,
            valid_points: 0,
            equivocated: 0,
            invalid_points: 0,
            ill_formed_points: 0,
            references_skipped: 0,
        }
    }
}

impl std::ops::AddAssign for PeerStats {
    fn add_assign(&mut self, rhs: Self) {
        self.first_round = self.first_round.min(rhs.first_round);
        self.last_round = self.first_round.max(rhs.last_round);
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
