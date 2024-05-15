## Code Style

### Idiomatic Rust

- _Consistency_: there's usually only one idiomatic solution amidst many
  non-idiomatic ones.
- _Predictability_: you can use the APIs without consulting documentation.
- _Performance, ergonomics and correctness_: language idioms usually reflect learned truths, which might not be immediately obvious.
- _Normalization_: reduce the number of possible invariants by using the type system and logic.
- _Awareness_: if in the process of writing code you thought about some little thing that could break something - either handle it or leave a TODO comment.

### Standard Naming

- Use `-` rather than `_` in crate names and in corresponding folder names.
- Avoid single-letter variable names especially in long functions. Common `i`, `j` etc. loop variables are somewhat of an exception but since Rust encourages use of iterators those cases aren’t that common anyway.
- Follow standard [Rust naming patterns](https://rust-lang.github.io/api-guidelines/naming.html) such as:
  - Don’t use `get_` prefix for getter methods. A getter method is one which returns (a reference to) a field of an object.
  - Use `set_` prefix for setter methods. An exception are builder objects which may use different a naming style.
  - Use `into_` prefix for methods which consume `self` and `to_` prefix for methods which don’t.

### Deriving traits (apply only to libraries)

Derive `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, and `Hash` for public types when possible (in this order).

**Rationale:** these traits can be useful for users and can be implemented for most types.

Derive `Default` when there is a reasonable default value for the type.

### Full paths for logging

Always write `tracing::<op>!(...)` instead of importing `use tracing::<op>;` and invoking `<op>!(...)`.

```rust
// GOOD
tracing::warn!("Everything is on fire");

// BAD
use tracing::warn;

warn!("Everything is on fire");
```

**Rationale:**

- Less polluted import blocks
- Uniformity

### [Tracing](https://tracing.rs)

When emitting events and spans with `tracing` prefer adding variable data via [`tracing`'s field mechanism](https://docs.rs/tracing/latest/tracing/#recording-fields).

```rust
// GOOD
debug!(
    target: "client",
    validator_id = self.client.validator_signer.as_ref().map(|vs| {
        tracing::field::display(vs.validator_id())
    }),
    %hash,
    "block.previous_hash" = %block.header().prev_hash(),
    "block.height" = block.header().height(),
    %peer_id,
    was_requested,
    "received block",
);
```

Most apparent violation of this rule will be when the event message utilizes any form of formatting, as seen in the following example:

```rust
// BAD
debug!(
    target: "client",
    "{:?} Received block {} <- {} at {} from {}, requested: {}",
    self.client.validator_signer.as_ref().map(|vs| vs.validator_id()),
    hash,
    block.header().prev_hash(),
    block.header().height(),
    peer_id,
    was_requested
);
```

Always specify the `target` explicitly. A good default value to use is the crate name, or the module path (e.g. `chain::client`) so that events and spans common to a topic can be grouped together. This grouping can later be used for customizing which events to output.

**Rationale:** This makes the events structured – one of the major value add propositions of the tracing ecosystem. Structured events allow for immediately actionable data without additional post-processing, especially when using some of the more advanced tracing subscribers. Of particular interest would be those that output events as JSON, or those that publish data to distributed event collection systems such as opentelemetry. Maintaining this rule will also usually result in faster execution (when logs at the relevant level are enabled.)

### Spans

Use the [spans](https://docs.rs/tracing/latest/tracing/#spans) to introduce context and grouping to and between events instead of manually adding such information as part of the events themselves. Most of the subscribers ingesting spans also provide a built-in timing facility, so prefer using spans for measuring the amount of time a section of code needs to execute.

Give spans simple names that make them both easy to trace back to code and to find a particular span in logs or other tools ingesting the span data. If a span begins at the top of a function, prefer giving it the name of that function; otherwise, prefer a `snake_case` name.

When instrumenting asynchronous functions the [`#[tracing::instrument]`][instrument] macro or the `Future::instrument` is **required**. Using `Span::entered` or a similar method that is not aware of yield points will result in incorrect span data and could lead to difficult to troubleshoot issues such as stack overflows.

Always explicitly specify the `level`, `target`, and `skip_all` options and do not rely on the default values. `skip_all` avoids adding all function arguments as span fields which can lead recording potentially unnecessary and expensive information. Carefully consider which information needs recording and the cost of recording the information when using the `fields` option.

[instrument]: https://docs.rs/tracing-attributes/latest/tracing_attributes/attr.instrument.html

```rust
#[tracing::instrument(
    level = "trace",
    target = "network",
    "handle_sync_routing_table",
    skip_all
)]
async fn handle_sync_routing_table(
    clock: &time::Clock,
    network_state: &Arc<NetworkState>,
    conn: Arc<connection::Connection>,
    rtu: RoutingTableUpdate,
) {
    ...
}
```

In regular synchronous code it is fine to use the regular span API if you need to instrument portions of a function without affecting the code structure:

```rust
fn compile_and_serialize_wasmer(code: &[u8]) -> Result<wasmer::Module> {
    // Some code...
    {
        let _span = tracing::debug_span!(target: "vm", "compile_wasmer").entered();
        // ...
        // _span will be dropped when this scope ends, terminating the span created above.
        // You can also `drop` it manually, to end the span early with `drop(_span)`.
    }
    // Some more code...
}
```

**Rationale:** Much as with events, this makes the information provided by spans structured and contextual. This information can then be output to tooling in an industry standard format, and can be interpreted by an extensive ecosystem of `tracing` subscribers.

### Event and span levels

The `INFO` level is enabled by default, use it for information useful for node operators. The `DEBUG` level is enabled on the canary nodes, use it for information useful in debugging testnet failures. The `TRACE` level is not generally enabled, use it for arbitrary debug output.

### Order of imports

```rust
// First core, alloc and/or std
use core::fmt;
use std::{...};

// Second, external crates (both crates.io crates and other rust-analyzer crates).
use crate_foo::{ ... };
use crate_bar::{ ... };

// Finally, the internal crate modules and submodules
use crate::{};
use super::{};
use self::y::Y;

// If applicable, the current sub-modules
mod x;
mod y;
```

### Import Style

When implementing traits from `core::fmt`/`std::fmt` import the module:

```rust
// GOOD
use core::fmt;

impl fmt::Display for RenameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { .. }
}

// BAD
impl core::fmt::Display for RenameError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result { .. }
}
```

When importing sub-modules:

```rust
// GOOD
use self::x::Y;

mod x;

// BAD
use x::Y;

mod x;
```

### If-let

Avoid the `if let ... { } else { }` construct if possible, use `match` instead:

```rust
// GOOD
match ctx.expected_type.as_ref() {
    Some(expected_type) => completion_ty == expected_type && !expected_type.is_unit(),
    None => false,
}

// BAD
if let Some(expected_type) = ctx.expected_type.as_ref() {
    completion_ty == expected_type && !expected_type.is_unit()
} else {
    false
}
```

Use `if let ... { }` when a match arm is intentionally empty:

```rust
// GOOD
if let Some(expected_type) = this.as_ref() {
    // Handle it
}

// BAD
match this.as_ref() {
    Some(expected_type) => {
        // Handle it
    },
    None => (),
}
```

### Push Ifs Up And Fors Down

[Please read this](https://matklad.github.io/2023/11/15/push-ifs-up-and-fors-down.html)

If there’s an if condition inside a function, consider if it could be moved to the caller instead:

```rust
// GOOD
fn frobnicate(walrus: Walrus) {
    ...
}

// BAD
fn frobnicate(walrus: Option<Walrus>) {
    let walrus = match walrus {
        Some(it) => it,
        None => return,
    };
    ...
}
```

If there's an if condition inside a for loop, consider splitting it into different loops:

```rust
// GOOD
if condition {
    for walrus in walruses {
        walrus.frobnicate()
    }
} else {
    for walrus in walruses {
        walrus.transmogrify()
    }
}

// BAD
for walrus in walruses {
    if condition {
        walrus.frobnicate()
    } else {
        walrus.transmogrify()
    }
}
```

Avoid deep nesting with early exit:

```rust
// GOOD
fn make_chevapi(ctx: Context) -> Result<Option<Chevapi>> {
    let Some(grill) = ctx.grill else {
        return Ok(None);
    };

    let Some(mut meat) = ctx.meat else {
        return Ok(None);
    };

    meat.verify_edible()?;

    loop {
        if let Some(chevapi) = grill.cook(&mut meat) {
            return Ok(Some(chevapi));
        }

        meat.flip();
    }
}

// BAD
fn make_chevapi(ctx: Context) -> Result<Option<Chevapi>> {
    Ok(if let Some(grill) = ctx.grill {
        if let Some(mut meat) = ctx.meat {
            meat.verify_edible()?;
            loop {
                if let Some(chevapi) = grill.cook(&mut meat) {
                    break Some(chevapi);
                } else {
                    meat.flip();
                }
            }
        } else {
            None
        }
    } else {
        None
    })
}
```

### Prefer for loops over `for_each` and `try_for_each` methods

Iterators offer `for_each` and `try_for_each` methods which allow executing a closure over all items of the iterator. This is similar to using a for loop but comes with various complications and may lead to less readable code. Prefer using a loop rather than those methods, for example:

```rust
// GOOD
for outcome_with_id in result? {
    *total_gas_burnt =
        safe_add_gas(*total_gas_burnt, outcome_with_id.outcome.gas_burnt)?;
    outcomes.push(outcome_with_id);
}

// BAD
result?.into_iter().try_for_each(
    |outcome_with_id: ExecutionOutcomeWithId| -> Result<(), RuntimeError> {
        *total_gas_burnt =
            safe_add_gas(*total_gas_burnt, outcome_with_id.outcome.gas_burnt)?;
        outcomes.push(outcome_with_id);
        Ok(())
    },
)?;
```

**Rationale:** The `for_each` and `try_for_each` methods don't play nice with `break` and `continue` statements, nor do they mesh well with async IO (since `.await` inside of the closure isn't possible). And while `try_for_each` allows for the use of the question mark operator, one may end up having to use it twice: once inside the closure and a second time outside the call to `try_for_each`. Furthermore, usage of these functions often introduces some minor syntactic noise.

There are situations when those methods may lead to more readable code. Common example are long call chains. Even then such code may evolve with the closure growing and leading to less readable code. If advantages of using the methods aren’t clear cut, it’s usually better to err on side of more imperative style.

Lastly, anecdotally, the methods (e.g., when used with `chain` or `flat_map`) may lead to faster code. This intuitively makes sense, but it's worth keeping in mind that compilers are pretty good at optimizing, and in practice, they may generate optimal code anyway. Furthermore, optimizing code for readability may be more important (especially outside of the hot path) than small performance gains.

### `&str` -> `String` conversion

Prefer using `.to_string()` or `.to_owned()`, rather than `.into()`, `String::from`, etc.

**Rationale:** uniformity, intent clarity.

### Prefer `to_string` to `format!("{}")`

Prefer calling `to_string` method on an object rather than passing it through
`format!("{}")` if all you’re doing is converting it to a `String`.

```rust
// GOOD
let hash = block_hash.to_string();
let msg = format!("{}: failed to open", path.display());

// BAD
let hash = format!("{block_hash}");
let msg = path.display() + ": failed to open";
```

**Rationale:** `to_string` is shorter to type and also faster.

### Prefer using enums instead of a bunch of `bool`s

Avoid using multiple bools when their meaning depends on each other:

```rust
// GOOD
enum InsertMode {
    Add,
    Replace,
    Set,
}

fn dict_insert(key: Key, value: Value, mode: InsertMode) {
    ...
}

// BAD
fn dict_insert(key: Key, value: Value, can_add: bool, can_replace: bool) {
    assert!(can_add || can_replace, "invalid params");
    ...
}
```

### Always keep an eye on the size of the structures

Do not pass large objects by value as arguments. Instead, use references or `Box`/`Arc`. Exceptions to this rule include various types of builders or cases where performance concerns are negligible.

**Rationale:** `memcpy` goes brr.

### Prefer clonnable structs instead of always wrapping into `Arc`

```rust
// GOOD
#[derive(Clone)]
pub struct MyObject {
    inner: Arc<Inner>
}

impl MyObject {
    pub fn new() -> Self {
        ...
    }
}

struct Inner {
    ...
}

// BAD
pub struct MyObject {
    ...
}

impl MyObject {
    pub fn new() -> Arc<Self> {
        ...
    }
}
```

**Rationale:** hiding an `Arc` inside the struct simplifies usage outside of this module.

### Leave comments

- Always mark `unsafe` blocks with `// SAFETY: why...`
- Mark performance critical paths with `// PERF: why...`
- Duplicate a complex algorithm with a simple text description of each step:

  ```rust
  // Read the next part of the key from the current data
  let prefix = &mut ok!(read_label(&mut remaining_data, key.remaining_bits()));

  // Match the prefix with the key
  let lcp = key.longest_common_data_prefix(prefix);
  match lcp.remaining_bits().cmp(&key.remaining_bits()) {
      // If all bits match, an existing value was found
      std::cmp::Ordering::Equal => break remaining_data.range(),
      // LCP is less than prefix, an edge to slice was found
      std::cmp::Ordering::Less if lcp.remaining_bits() < prefix.remaining_bits() => {
          return Ok(None);
      }
      // The key contains the entire prefix, but there are still some bits left
      std::cmp::Ordering::Less => {
          // Fail fast if there are not enough references in the fork
          if data.reference_count() != 2 {
              return Err(Error::CellUnderflow);
          }

          // Remove the LCP from the key
          prev_key_bit_len = key.remaining_bits();
          key.try_advance(lcp.remaining_bits(), 0);

          // Load the next branch
          let next_branch = Branch::from(ok!(key.load_bit()));

          let child = match data.reference(next_branch as u8) {
              // TODO: change mode to `LoadMode::UseGas` if copy-on-write for libraries is not ok
              Some(child) => ok!(context.load_dyn_cell(child, LoadMode::Full)),
              None => return Err(Error::CellUnderflow),
          };

          // Push an intermediate edge to the stack
          stack.push(Segment {
              data,
              next_branch,
              key_bit_len: prev_key_bit_len,
          });
          data = child;
      }
      std::cmp::Ordering::Greater => {
          panic!(false, "LCP of prefix and key can't be greater than key");
      }
  }
  ```

### Import Granularity

Group imports by module, but not deeper:

```rust
// GOOD
use std::collections::{hash_map, BTreeSet};
use std::sync::Arc;

// BAD - nested groups.
use std::{
    collections::{hash_map, BTreeSet},
    sync::Arc,
};

// BAD - not grouped together.
use std::collections::BTreeSet;
use std::collections::hash_map;
use std::sync::Arc;
```

This corresponds to `"rust-analyzer.assist.importGranularity": "module"` setting
in rust-analyzer
([docs](https://rust-analyzer.github.io/manual.html#rust-analyzer.assist.importGranularity)).

**Rationale:** Consistency, matches existing practice.

### Use `Self` where possible

When referring to the type for which block is implemented, prefer using `Self`, rather than the name of the type:

```rust
impl ErrorKind {
    // GOOD
    fn print(&self) {
        Self::Io => println!("Io"),
        Self::Network => println!("Network"),
        Self::Json => println!("Json"),
    }

    // BAD
    fn print(&self) {
        ErrorKind::Io => println!("Io"),
        ErrorKind::Network => println!("Network"),
        ErrorKind::Json => println!("Json"),
    }
}
```

```rust
impl<'a> AnswerCallbackQuery<'a> {
    // GOOD
    fn new<C>(bot: &'a Bot, callback_query_id: C) -> Self
    where
        C: Into<String>,
    { ... }

    // BAD
    fn new<C>(bot: &'a Bot, callback_query_id: C) -> AnswerCallbackQuery<'a>
    where
        C: Into<String>,
    { ... }
}
```

**Rationale:** `Self` is generally shorter and it's easier to copy-paste code or rename the type.

### Arithmetic integer operations

Use methods with an appropriate overflow handling over plain arithmetic operators (`+-*/%`) when dealing with integers.

```
// GOOD
a.wrapping_add(b);
c.saturating_sub(2);
d.widening_mul(3);   // NB: unstable at the time of writing
e.overflowing_div(5);
f.checked_rem(7);

// BAD
a + b
c - 2
d * 3
e / 5
f % 7
```

If you’re confident the arithmetic operation cannot fail, `x.checked_[add|sub|mul|div](y).expect("explanation why the operation is safe")` is a great alternative, as it neatly documents not just the infallibility, but also _why_ that is the case.

This convention may be enforced by the `clippy::arithmetic_side_effects` and `clippy::integer_arithmetic` lints.

**Rationale:** By default the outcome of an overflowing computation in Rust depends on a few factors, most notably the compilation flags used. The quick explanation is that in debug mode the computations may panic (cause side effects) if the result has overflowed, and when built with optimizations enabled, these computations will wrap-around instead.

### Metrics

Consider adding metrics to new functionality. For example, how often each type of error was triggered, how often each message type was processed.

**Rationale:** Metrics are cheap to increment, and they often provide a significant insight into operation of the code, almost as much as logging. But unlike logging metrics don't incur a significant runtime cost.

### Naming

Prefix all `tycho` metrics with `tycho_`. Follow the [Prometheus naming convention](https://prometheus.io/docs/practices/naming/) for new metrics.

**Rationale:** The `tycho_` prefix makes it trivial to separate metrics exported by `tycho` from other metrics, such as metrics about the state of the machine that runs `tycho`.

All time measurements must end with the `_sec` postfix:

```rust
metrics::histogram!("tycho_metric_name_sec").record(elapsed_time)
```

**Rationale:** The `_sec` postfix is handled by the collector, which transforms it into a lightweight histogram. Otherwise, the collector will try to build a complex summary. [Read more](https://prometheus.io/docs/practices/histograms/#quantiles)

### Metrics performance

In most cases incrementing a metric is cheap enough never to give it a second thought. However accessing a metric with labels on a hot path needs to be done carefully.

If a label is based on an integer, use a faster way of converting an integer to the label, such as the `itoa` crate.

For hot code paths, re-use results of `with_label_values()` as much as possible.

**Rationale:** We've encountered issues caused by the runtime costs of incrementing metrics before. Avoid runtime costs of incrementing metrics too often.

### Locking in `async` context

It is ok to use `std`/`parking_lot` `Mutex`/`RwLock` inside short-lived sync blocks. Just make sure to not leave guards to live across `.await`.
Use `tokio::sync::Mutex` to synchronise tasks (control flow).

```rust
// GOOD
struct SharedData {
    state: std::sync::Mutex<SimpleState>,
}

async fn some_process(shared: &SharedData) {
    ...
    loop {
        ...
        {
            let mut state = shared.state.lock().unwrap();
            state.fast_operation();
        }

        some_io().await;
        ...
    }
}

// BAD
struct SharedData {
    state: tokio::sync::Mutex<SimpleState>,
}

async fn some_process(shared: &SharedData) {
    ...
    loop {
        ...
        {
            let mut state = shared.state.lock().await;
            state.fast_operation();
        }

        some_io().await;
        ...
    }
}

```

Make sure that sync blocks don't occupy the thread for too long. Use `spawn_blocking` for complex computations or file IO.

```rust
// GOOD
async fn verify_block(node: &Node, block: &Block) -> Result<()> {
    // NOTE: signatures.len() > 100
    let signatures = node.get_signatures(block).await?;
    tokio::task::spawn_blocking(move || {
        for signature in signatures {
            signature.verify()?;
        }
        Ok(())
    })
    .await
    .unwrap()
}

// BAD
async fn verify_block(node: &Node, block: &Block) -> Result<()> {
    let signatures = node.get_signatures(block).await?;

    for signature in signatures {
        signature.verify()?;
    }
    Ok(())
}
```

### Try to make your futures cancel safe (TODO)

### Don't make big futures (TODO)

### DashMap common pitfalls (TODO)

### Atomics pitfalls (TODO)

[Read this book!](https://marabos.nl/atomics/preface.html)
