## Pull Request Checklist

### NODE CONFIGURATION CHANGES
- [ ] Node configuration changes included
    - [ ] **If yes:**  
        - [ ] Described all changes, default values, and rationale
        - [ ] Confirmed new node starts correctly with old configuration

---

### BLOCKCHAIN CONFIGURATION CHANGES
- [ ] Blockchain configuration changes included
    - [ ] **If yes:**  
        - [ ] Described all changes, default values, and rationale
        - [ ] Confirmed new node starts correctly with old blockchain state

---

### COMPATIBILITY
- [ ] All compatibility-sensitive features checked:
    - [ ] State
    - [ ] State. Blockchain Config
    - [ ] Block
    - [ ] Block Proof
    - [ ] Archive
    - [ ] Persistent State
    - [ ] Persistent Queue
    - [ ] Storage. Blocks
    - [ ] Storage. States
    - [ ] Storage. Archives
    - [ ] Queue. Diff
    - [ ] Queue. Storage
    - [ ] Queue. Storage. Statistics
    - [ ] Message Processing Algorithm
    - [ ] ProcessedUpto
    - [ ] Collator. Limits
    - [ ] Collator. Work Units Calculation
    - [ ] Mempool. Consensus Config
    - [ ] Mempool. Validation rules
    - [ ] Mempool. Storage
    - [ ] Mempool. API tl models
- [ ] For each affected feature, specified:
    - [ ] Compatibility status: [fully compatible / special logic applied / incompatible]
    - [ ] Description of compatibility or migration instructions
    - [ ] Description of compatibility testing

---

### SPECIAL DEPLOYMENT ACTIONS
- [ ] Special deployment actions required
    - [ ] **If required:**  
        - [ ] Described safe update steps and timing
        - [ ] Stated if backup is required
        - [ ] Stated if nodes generate invalid blocks until 2/3+1 updated
        - [ ] Provided tested update scripts (if applicable)

---

### PERFORMANCE IMPACT
- [ ] No impact expected
- [ ] Impact expected
    - [ ] **If yes:**  
        - [ ] Described expected changes and rationale
        - [ ] Noted new metrics (if any)
        - [ ] Linked separate optimization task (if applicable)
        - [ ] Attached comparative devnet test results (screenshots, Grafana links)

---

### TESTS
#### Unit Tests
- [ ] Covered by unit tests
- [ ] Corresponding tests added/updated
- [ ] Additional test tasks created and linked (if needed)

#### Network Tests
- [ ] Covered by network tests
- [ ] Corresponding tests added/updated
- [ ] Additional test tasks created and linked (if needed)

#### Manual Tests
- [ ] Manual tests used:
    - [ ] 1-255, transfers, swaps, others (list or describe as needed)
    - [ ] If new manual test, instructions included
    - [ ] Provided Grafana link with devnet metrics and attached screenshots (if notable changes)

---

**Notes/Additional Comments:**  
<!-- Add any additional information or context for reviewers here. -->
