@pure
fun maxInt(): int
    asm "256 PUSHPOW2DEC"

@inline_ref
fun cell?.hashEq(self, expectedVsetHash: int): bool {
    if (self == null) {
        return false;
    } else {
        return expectedVsetHash == self.hash();
    }
}
