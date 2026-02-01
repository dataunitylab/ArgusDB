# Lazy Deserialization Specification

## Objective
Reduce CPU and memory overhead during query execution by deferring the deserialization of JSONB documents until absolutely necessary. Currently, `JSTableIterator` fully deserializes every document into a `Value` (allocating a `BTreeMap` and `String`s) even if the document is immediately discarded by a filter.

## Context & Constraints
- **Performance**: Profiling shows `make_static` (deserialization/allocation) is the primary bottleneck.
- **Data Format**: `jsonb_schema` encoding is used.
- **Schema Encoding**: The user has noted that the encoding may depend on the schema (e.g., `minimum: 100` might result in `189` being stored as `89`).
- **Correctness**: We must ensure that predicates (e.g., `> 189`) are evaluated correctly against the *logical* values, not the raw *stored* values, unless we explicitly adjust the predicates.

## Proposed Strategy: Decode-on-Demand

We will implement a **Decode-on-Demand** strategy. Instead of converting the raw JSONB bytes into a heavy `Value` tree immediately, we will pass a lightweight handle to the raw bytes (`LazyDocument`) through the operator pipeline. Deserialization will occur only for the specific fields required by the query, and only at the moment they are needed.

This strategy avoids the complexity of rewriting query predicates (to match raw stored values) by relying on the `jsonb_schema` decoder to restore the correct logical values for individual fields.

### 1. Data Structures

Introduce a `LazyDocument` to wrap the raw binary data.

```rust
pub struct LazyDocument {
    // The raw JSONB blob for the document [id, doc]
    // Or potentially just the 'doc' part if we split them early.
    pub raw: Vec<u8>,
    // We might need to keep the ID separate to avoid decoding it repeatedly if it's frequent.
    pub id: String,
}

// Update the Item type for Operators
pub enum ExecutionResult {
    // Standard fully deserialized value (for compatibility or final output)
    Value(String, Value),
    // Lazy wrapper
    Lazy(LazyDocument),
}
```

*Note: For the initial implementation, we might just change `Iterator<Item=(String, Value)>` to `Iterator<Item=(String, LazyDocument)>` internally if possible, or use a new Trait.*

### 2. Operator Modifications

#### `ScanOperator` & `JSTableIterator`
- **Current**: Reads bytes -> `from_slice` -> `make_static` -> Yields `Value`.
- **New**: Reads bytes -> Yields `LazyDocument { id, raw }`.
- `JSTableIterator` will parse the outer array `[id, doc]` structure *raw* (using `jsonb` navigation) to extract the `id` string (needed for the tuple) and the `doc` byte slice, without decoding the `doc` body.

#### `FilterOperator`
- **Current**: Receives `Value`. Evaluates `Expression` against `Value`.
- **New**: Receives `LazyDocument`.
- `evaluate_expression` must be updated to handle `LazyDocument`.
    - It uses `jsonb_schema`'s raw navigation APIs (e.g., `select_by_path`) to locate the bytes for the referenced field.
    - **Crucial Step**: It calls `from_slice` *on just that field's bytes*.
    - This returns a small, temporary `Value` (e.g., a single Integer).
    - The predicate is evaluated against this logical `Value`.
    - If the predicate passes, the `LazyDocument` is yielded. If not, it is dropped (and the full document was never allocated).

#### `ProjectOperator`
- **Current**: Constructs a new `BTreeMap` from the source `Value`.
- **New**: Receives `LazyDocument`.
- Iterates over projection expressions.
- Uses the same "extract & decode" logic to build the result `Value`.
- **Note**: Since `Project` typically creates a *new* structure, it effectively materializes the result. This is acceptable; we saved the cost of deserializing non-projected fields.

### 3. Safety vs. Raw Comparison
The user warned about schema-based encoding (e.g., min-value subtraction).
- **Our Approach**: By calling `from_slice` on the extracted field bytes, we let the `jsonb_schema` library handle the decoding logic (adding back the offset, etc.).
- **Result**: We compare `Logical(189)` > `Query(189)`. This is correct.
- **Alternative (Not chosen yet)**: Comparing `Raw(89)` > `AdjustedQuery(89)`. This requires deep knowledge of the encoding and schema, and extensive predicate rewriting logic. We will avoid this complexity for "Stage 1" of this optimization unless performance is still insufficient.

## Implementation Steps

1.  **Define `LazyDocument`**: Create the struct in `src/db.rs` or `src/query.rs`.
2.  **Update `JSTableIterator`**: Modify it to return `LazyDocument` (or a type that can be one).
    *   *Challenge*: The current `Iterator` trait is `Item = io::Result<(String, Value)>`. We need to change the return type of `execute_plan` and all operators.
    *   *Refactoring*: Change `execute_plan` to return `Box<dyn Iterator<Item = Result<LazyDocument, ...>>>` or similar.
    *   For backward compatibility with the `main` function (which expects `Value` to print/serialize), the final step (consumption) or a top-level `Materialize` operator can convert `LazyDocument` -> `Value`.
3.  **Update Evaluator**: Add `evaluate_lazy(&Expression, &LazyDocument) -> Value`.
4.  **Refactor Operators**: Update `Scan`, `Filter`, `Project`, `Limit`, `Offset` to generic or updated iterator types.

## Risk Management
- **Schema Access**: If `from_slice` *requires* the schema to decode correctly (it shouldn't if the blob is self-contained, but if it does), we need to ensure `LazyDocument` holds a reference to the `Schema`.
    - *Investigation*: `JSTableIterator` has access to `self.schema`. We can pass `&Schema` to `LazyDocument` if needed.
    - *Assumption*: `jsonb_schema::from_slice` (used currently in `make_static`) takes no schema argument, so the blob is self-describing or standard encoding. We will proceed with this assumption.
