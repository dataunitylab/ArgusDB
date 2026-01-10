The syntax used by ArgusDB is an extension of SQL and inspired by [XTDB](https://xtdb.com/).
Examples of syntax for different use cases are given below.

## Insertion

```sql
INSERT INTO people
RECORDS {_id: 6,
         name: 'fred',
         info: {contact: [{loc: 'home',
                           tel: '123'},
                          {loc: 'work',
                           tel: '456',
                           registered: DATE '2024-01-01'}]}}
```

## Querying

```sql
SELECT (people.info).contact[2].tel
FROM people
WHERE people.name = 'fred'
```
