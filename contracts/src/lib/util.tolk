@pure
fun maxInt(): int
    asm "256 PUSHPOW2DEC"

@pure
fun getPreviousMcBlocks(): tuple
    asm "PREVMCBLOKS"

@inline_ref
fun cell?.hashEq(self, expectedVsetHash: int): bool {
    if (self == null) {
        return false;
    } else {
        return expectedVsetHash == self.hash();
    }
}

/// Returns the first `0 ≤ bits ≤ 1023` bits and first `0 ≤ refs ≤ 4` references of a slice.
@pure
fun slice.getFirstBitsAndRefs(self, bits: int, refs: int): slice
    asm "SCUTFIRST"

@pure
fun slice.getFirstAny<T>(self): slice {
    val original = self;
    var cs = self;
    var (bits, refs) = cs.remainingBitsAndRefsCount();
    cs.loadAny<T>();
    val (tailBits, tailRefs) = cs.remainingBitsAndRefsCount();
    return original.getFirstBitsAndRefs(bits - tailBits, refs - tailRefs);
}
