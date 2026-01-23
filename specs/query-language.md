# Query Language Specification

ArgusDB supports a SQL-like query language optimized for manipulating and retrieving JSON documents.

## Data Model

The database consists of collections of JSON documents. Each document is a semi-structured object with nested fields, arrays, and values.

## Statements

### CREATE COLLECTION

Creates a new, empty collection in the database.

**Syntax:**

```sql
CREATE COLLECTION <collection_name>
```

### DROP COLLECTION

Removes an entire collection, including all of its documents and associated data.

**Syntax:**

```sql
DROP COLLECTION <collection_name>
```

### SHOW COLLECTIONS

Lists all available collections in the database.

**Syntax:**

```sql
SHOW COLLECTIONS
```

### INSERT

The `INSERT` statement is used to add new documents to a collection.

**Syntax:**

```sql
INSERT INTO <collection_name>
VALUES (`json_object`) [, (`json_object`) ...]
```

**Parameters:**

*   `collection_name`: The name of the collection to insert into.
*   `json_object`: A standard JSON object literal enclosed in backticks (`). This allows for unescaped JSON content to be embedded directly in the query.

**Example:**

```sql
INSERT INTO people
VALUES (`{"name": "Alice", "age": 30, "address": {"city": "Paris", "zip": "75001"}}`)
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
*   **JSONPath**:
    *   Identifiers starting with `$` are treated as JSONPath expressions.
    *   Example: `$.store.book[0].title`
    *   Complex paths containing special characters (like brackets `[]`) should be enclosed in backticks: `` `$.store.book[0].title` ``.
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
SELECT name, `$.info.contact[0].tel`
FROM people
WHERE `$.age` >= 21 AND active = TRUE
LIMIT 10
OFFSET 5
```
#### Functions

There are a number of predefined functions that can be used in the `SELECT` clause.
Each of these takes either a field name or a JSONPath expression as an argument.

##### Unary Functions
- `ABS(x)`: Returns the absolute value of `x`
- `ACOS(x)`: Returns the arc cosine of `x` in radians
- `ASIN(x)`: Returns the arc sine of `x` in radians
- `ATAN(x)`: Returns the arc tangent of `x` in radians
- `CEIL(x)`: Returns the ceiling of `x`
- `FLOOR(x)`: Returns the floor of `x`
- `LN(x)`: Returns the natural logarithm of `x`
- `SIN(x)`: Returns the sine of `x` in radians
- `TAN(x)`: Returns the tangent of `x` in radians
- `SQRT(x)`: Returns the square root of `x`
