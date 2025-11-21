# Multi-field cursor pagination implementation for Django Rest Framework.
## Allows multiple fields to be used for cursor pagination

The built-in cursor pagination in DRF relies on the main field that you're
paginating on to be 'sufficiently' unique, even if multiple fields are provided to
use as the ordering (and those fields guarantee unique ordering). This is an
update, posted orignally as a PR against the DRF project by
[sonthonaxrk](https://github.com/sonthonaxrk).

Relevant links:
- https://www.django-rest-framework.org/api-guide/pagination/#cursorpagination
- https://github.com/sonthonaxrk/django-rest-framework/blob/29d8796b1d96cbe77ecd81663ee7afbace0229e0/rest_framework/pagination.py
- https://github.com/encode/django-rest-framework/discussions/7888

The use case this solved for me was ordering on time (and id) where the times
were not guarunteed to be unique due to batch creation.

## Small extension
In addition to the options described on the normal CursorPagintation docs - the option to
write the where clause as a tuple comparsion was added. It's an implementation detail and
will only work on drivers that support it.

To try it, set the `use_tuple_comparison` attribute to True to enable tuple comparison for
the cursor. This was found to be significantly more performant on Postgres.

## Running Tests

```
uv run pytest
```


