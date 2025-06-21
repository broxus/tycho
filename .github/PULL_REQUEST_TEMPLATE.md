## Pull Request Checklist

### NODE CONFIGURATION MODEL CHANGES

[None] / [Yes]

<!--Describe changes, default values, and rationale.-->
<!--!!! Confirm that the new node version starts correctly with old configuration.-->

### BLOCKCHAIN CONFIGURATION MODEL CHANGES

[None] / [Yes]

<!--Describe changes, default values, and rationale.-->
<!--!!! Confirm that the new node version starts correctly with blockchain state.-->

---

### COMPATIBILITY

<!--Is the new version compatible with the previous state of the node and blockchain? Specify which compatibility-sensitive features were modified. Possible list below. If a feature is not affected, do not list it.-->

Affected features:

- [State]
- [State. Blockchain Config]
- [Block]
- [Block Proof]
- [Archive]
- [Persistent State]
- [Persistent Queue]
- [Storage. Blocks]
- [Storage. States]
- [Storage. Archives]
- [Queue. Diff]
- [Queue. Storage]
- [Queue. Storage. Statistics]
- [Message Processing Algorithm]
- [ProcessedUpto]
- [Collator. Limits]
- [Collator. Work Units Calculation]
- [Mempool. Consensus Config]
- [Mempool. Validation rules]
- [Mempool. Storage]
- [Mempool. API tl models]

<!--
For each affected feature specify:
  - Compatibility status: [fully compatible] / [special logic applied] / [incompatible]
  - If not compatible, provide migration instructions
  - If compatibility ensured, describe how. Describe how compatibility was tested.
-->

### SPECIAL DEPLOYMENT ACTIONS

[Not Required] / [Required]

<!--
If required:
  - Described safe update steps and timing
  - Described required configuration changes
  - State if nodes will generate invalid blocks until 2/3+1 updated
  - Provided tested update scripts (if applicable)
-->

---

### PERFORMANCE IMPACT

[No impact expected] / [Expected impact]

<!--
If impact expected:
  - Described expected changes and rationale
  - Describe new added metrics (if any)
  - Attached comparative devnet test results (screenshots, Grafana links)
  - Link separate optimization task (if applicable)
-->

---

### TESTS

#### Unit Tests

[Covered by:] / [No coverage]

<!--List unit tests that cover changes (if exits). Link tasks to create additional tests (if needed).-->

#### Network Tests

[Covered by:] / [No coverage]

<!--List unit tests that cover changes (if exits). Link tasks to create additional tests (if needed).-->

#### Manual Tests

<!--Describe how changes were manually tested (if were).-->

[Manual tests used]

- [1-255]
- [transfers]
- [swaps]
<!--List other tests if used.-->

<!--If a new test was used, describe how to run it.-->
<!--Provide Grafana links to tests runs on devnet. Attach screenshots to highlight notable changes.-->

---

**Notes/Additional Comments:**  
<!-- Add any additional information or context for reviewers here. -->
