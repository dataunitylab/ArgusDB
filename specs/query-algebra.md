# Internal Query Algebra

This document defines the operators and structures for the internal query algebra of ArgusDB. This algebra represents the intermediate representation of queries that will be executed against the database.

## Query Plan

A query plan is a tree of operators. The execution engine evaluates this tree, typically starting from the leaves (data sources) and processing data up to the root.

## Operators

The following operators are defined:

### 1. Scan

*   **Description**: Scans the database for documents. It produces a stream of documents.
*   **Parameters**:
    *   `collection`: (Implicitly the whole DB for now, or filtered by some criteria if we add collections later).
*   **Output**: A stream of `(DocumentID, Document)` pairs.

### 2. Project (Select)

*   **Description**: Transforms each document in the input stream by selecting a subset of fields or computing new fields.
*   **Parameters**:
    *   `projections`: A list of expressions defining the output fields.
*   **Input**: A stream of documents.
*   **Output**: A stream of modified documents.

### 3. Filter (Where)

*   **Description**: Filters the input stream, passing only documents that satisfy a predicate.
*   **Parameters**:
    *   `predicate`: A boolean expression that is evaluated against each document.
*   **Input**: A stream of documents.
*   **Output**: A stream of documents satisfying the predicate.

### 4. Limit

*   **Description**: Limits the number of documents returned.
*   **Parameters**:
    *   `limit`: The maximum number of documents to return.
*   **Input**: A stream of documents.
*   **Output**: A stream of at most `limit` documents.

### 5. Offset

*   **Description**: Skips a specified number of documents from the beginning of the stream.
*   **Parameters**:
    *   `offset`: The number of documents to skip.
*   **Input**: A stream of documents.
*   **Output**: The input stream minus the first `offset` documents.

## Expressions

Operators like `Project` and `Filter` rely on expressions.

*   **FieldReference**: Refers to a field in the document (e.g., `a.b`).
*   **Literal**: A constant value (e.g., `1`, `"hello"`, `true`).
*   **BinaryExpression**: Combines two expressions with an operator (e.g., `a > 5`, `b == "test"`).
    *   Supported operators: `=`, `!=`, `<`, `<=`, `>`, `>=`.
*   **LogicalExpression**: Combines boolean expressions.
    *   Supported operators: `AND`, `OR`, `NOT`.

## Execution Model

The query engine will execute the plan by pulling data from the root operator. Each operator pulls data from its child, processes it, and returns it to its parent. This is a standard iterator (Volcano) model.
