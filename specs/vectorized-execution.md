# Vectorized Execution Plan

## Goal
Implement vectorized execution to improve query performance by processing documents in batches. This improves CPU cache locality and enables the compiler to use SIMD instructions for filtering and projection.

## Design

### 1. Batch Structure
We introduce a `Batch` struct that holds a chunk of data to be processed together.

```rust
pub struct Batch {
    // Row-oriented batching of LazyDocuments (ExecutionResult)
    pub items: Vec<ExecutionResult>,
}
```

### 2. Iterator Model
Operators produce and consume `Batch` objects.

- `BatchIterator` trait (effectively `Iterator<Item = Batch>`).
- Batch size: Defaults to 4096, but adaptable via hints (e.g. for `LIMIT` queries).

### 3. Vectorized Operators

#### BatchScanOperator
- Reads from the underlying `MergedIterator`.
- Accumulates `N` items into a `Batch`.
- **Optimization:** Accepts a `batch_size` parameter (derived from `LIMIT` hint) to avoid over-fetching data from storage.

#### BatchFilterOperator (SIMD Target)
- **Input:** `Batch` of `LazyDocument`s.
- **Process:**
    1. Check if the predicate is suitable for vectorization (currently Simple Binary Numeric expressions).
    2. **Column Extraction:** Iterate through the batch and extract the specific field for all documents into a reusable typed buffer (`Vec<f64>`).
        - **Optimization:** Uses `evaluate_to_f64_lazy` (in `src/expression.rs`) to extract values directly from raw JSONB bytes without allocating intermediate `Value` enums or `BTreeMap`s.
    3. **SIMD Evaluation:** Perform the comparison loop over the extracted vector.
        - Relies on Rust/LLVM auto-vectorization for tight loops over primitive arrays.
    4. **Selection:** Filter the `Batch` in-place using the computed mask.
- **Fallback:** If the predicate is complex (e.g. `OR`, nested paths, non-numeric), the execution planner falls back to the standard Row-based execution plan (`execute_row_plan`) to ensure no performance regression.

#### BatchProjectOperator
- Iterate over the `Batch`.
- Apply projection to each item map-style.
- (Currently disabled for automatic vectorization selection to ensure stability, effectively using Row-based plan for Projections).

#### BatchLimitOperator / BatchOffsetOperator
- Handle `LIMIT` and `OFFSET` directly on `Batch` streams to avoid switching contexts.

### 4. Execution Strategy (`execute_plan`)

The `execute_plan` function now intelligently chooses between Vectorized and Row-based execution:

1.  **Check Vectorizability:** Analyzes the logical plan. If the plan consists of Scan and Simple Numeric Filters (and optionally Limit/Offset), it qualifies for vectorization.
2.  **Vectorized Path (`execute_batch_plan`):**
    - Constructs a pipeline of `Batch*` operators.
    - Propagates `LIMIT` values as hints to `BatchScanOperator`.
    - **Disable Pushdown:** Deliberately avoids pushing the predicate down to the storage engine (`db.scan`) for these simple cases. This forces the data into `BatchFilterOperator` where the efficient SIMD loop and allocation-free extraction can outperform the storage engine's row-by-row check.
3.  **Row-based Path (`execute_row_plan`):**
    - Used for complex queries (logical operators, projections, complex paths).
    - Utilizes standard `ScanOperator`, `FilterOperator`, etc.
    - Leverages full predicate pushdown to `MergedIterator`.

## Optimization Strategy
- **Memory Reuse:** `BatchFilterOperator` reuses `buf_values` and `buf_valid` vectors between batches to avoid allocation churn.
- **Allocation-Free Extraction:** `evaluate_to_f64_lazy` bypasses `Value` creation.
- **Adaptive Batching:** `BatchScanOperator` scales batch size based on query limits.
