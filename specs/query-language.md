# Query Language Specification

ArgusDB supports a SQL-like query language optimized for manipulating and retrieving JSON documents.

## Data Model

The database consists of collections of JSON documents. Each document is a semi-structured object with nested fields, arrays, and values.

## Statements

### INSERT

The `INSERT` statement is used to add new documents to a collection.

**Syntax:**

```sql
INSERT INTO <collection_name>
VALUES `json_object` [, `json_object` ...]
```

**Parameters:**

*   `collection_name`: The name of the collection to insert into.
*   `json_object`: A standard JSON object literal enclosed in backticks (`). This allows for unescaped JSON content to be embedded directly in the query.

**Example:**

```sql
INSERT INTO people
VALUES `{"name": "Alice", "age": 30, "address": {"city": "Paris", "zip": "75001"}}`
```

### SELECT

The `SELECT` statement retrieves data from a collection, allowing for filtering, projection, and pagination.

**Syntax:**

```sql
SELECT <expression_list>
FROM <collection_name>
[WHERE <predicate>]
[LIMIT <integer>]
[OFFSET <integer>]
```

#### Clauses

*   **SELECT**: Specifies the fields or expressions to return in the result set.
*   **FROM**: Specifies the source collection to query.
*   **WHERE**: Filters documents based on a boolean predicate. Only documents for which the predicate evaluates to `TRUE` are included in the result.
*   **LIMIT**: Restricts the maximum number of documents returned.
*   **OFFSET**: Skips a specified number of documents before returning results.

#### Expressions and Operators

The language supports various expressions to interact with JSON data:

*   **Field Access**:
    *   Dot notation: `info.contact` accesses the `contact` field within the `info` object.
    *   Array Indexing: `contact[0]` accesses the first element of the `contact` array.
*   **Literals**:
    *   Strings: `'value'` (single quotes) or `"value"` (double quotes)
    *   Numbers: `123`, `45.67`
    *   Booleans: `TRUE`, `FALSE`
    *   Null: `NULL`
*   **Comparison Operators**:
    *   `=`: Equality
    *   `!=` or `<>`: Inequality
    *   `<`, `<=`: Less than, Less than or equal
    *   `>`, `>=`: Greater than, Greater than or equal
*   **Logical Operators**:
    *   `AND`: Logical conjunction
    *   `OR`: Logical disjunction
    *   `NOT`: Logical negation

**Example:**

```sql
SELECT name, info.contact[0].tel
FROM people
WHERE age >= 21 AND active = TRUE
LIMIT 10
OFFSET 5
```