import operator
from base64 import b64decode, b64encode
from collections import namedtuple
from functools import reduce
from urllib import parse

from django.db.models.query import Q
from django.db.models import Func, F, TextField, Value
from django.db.models.functions import Cast

from rest_framework.exceptions import NotFound
from rest_framework.utils import json
from rest_framework.utils.urls import replace_query_param
from rest_framework.pagination import CursorPagination


def _positive_int(integer_string, strict=False, cutoff=None):
    """
    Cast a string to a strictly positive integer.
    """
    ret = int(integer_string)
    if ret < 0 or (ret == 0 and strict):
        raise ValueError()
    if cutoff:
        return min(ret, cutoff)
    return ret


def _reverse_ordering(ordering_tuple):
    """
    Given an order_by tuple such as `('-created', 'uuid')` reverse the
    ordering and return a new tuple, eg. `('created', '-uuid')`.
    """

    def invert(x):
        return x[1:] if x.startswith("-") else "-" + x

    return tuple([invert(item) for item in ordering_tuple])


Cursor = namedtuple("Cursor", ["offset", "reverse", "position"])
PageLink = namedtuple("PageLink", ["url", "number", "is_active", "is_break"])

PAGE_BREAK = PageLink(url=None, number=None, is_active=False, is_break=True)


class MultiFieldCursorMixin:
    """
    Multi-field cursor pagination implementation for Django Rest Framework.

    Allows multiple fields to be used for cursor pagination.

    This is from: https://github.com/sonthonaxrk/django-rest-framework/blob/29d8796b1d96cbe77ecd81663ee7afbace0229e0/rest_framework/pagination.py

    Meant to resolve: https://github.com/encode/django-rest-framework/discussions/7888

    Original implementation and tests by: https://github.com/sonthonaxrk

    Use the `use_tuple_comparison` attribute to enable tuple comparison for the cursor. This
    was found to be more performant on Postgres with a larget dataset, but may not be
    supported by other database drivers.
    """

    # use tuple comparison for the cursor where clauses
    use_tuple_comparison = False

    def paginate_queryset(self, queryset, request, view=None):
        self.page_size = self.get_page_size(request)
        if not self.page_size:
            return None

        self.base_url = request.build_absolute_uri()
        self.ordering = self.get_ordering(request, queryset, view)

        self.cursor = self.decode_cursor(request)
        if self.cursor is None:
            (offset, reverse, current_position) = (0, False, None)
        else:
            (offset, reverse, current_position) = self.cursor

        # Cursor pagination always enforces an ordering.
        if reverse:
            queryset = queryset.order_by(*_reverse_ordering(self.ordering))
        else:
            queryset = queryset.order_by(*self.ordering)

        # If we have a cursor with a fixed position then filter by that.
        if current_position is not None:
            current_position_list = json.loads(current_position)

            if self.should_use_tuple_comparison():
                # This is an optimization to avoid the big string of "
                # "OR (A = X and B > Y)" below and re-write as "(A, B) > (X, Y)"
                # It seems odd but PG used the same indices in different ways that
                # resulted in much better performance with the tuple comparison.
                field_names = [o.lstrip("-") for o in self.ordering]

                lhs = Func(
                    *[F(f) for f in field_names],
                    template="(%(expressions)s)",
                    output_field=TextField(),
                )

                cleaned_values = []
                for val, field_name in zip(current_position_list, field_names):
                    model_field = queryset.model._meta.get_field(field_name)
                    cleaned_values.append(Cast(Value(val), output_field=model_field))
                rhs = Func(
                    *cleaned_values,
                    template="(%(expressions)s)",
                    output_field=TextField(),
                )

                is_moving_forward = not self.cursor.reverse
                should_use_gt = (self._all_ascending() and is_moving_forward) or (
                    self._all_descending() and not is_moving_forward
                )

                queryset = queryset.alias(_cursor_tuple=lhs)

                if should_use_gt:
                    queryset = queryset.filter(_cursor_tuple__gt=rhs)
                else:
                    queryset = queryset.filter(_cursor_tuple__lt=rhs)
            else:
                q_objects_equals = {}
                q_objects_compare = {}

                for order, position in zip(self.ordering, current_position_list):
                    is_reversed = order.startswith("-")
                    order_attr = order.lstrip("-")

                    q_objects_equals[order] = Q(**{order_attr: position})

                    # Test for: (cursor reversed) XOR (queryset reversed)
                    if self.cursor.reverse != is_reversed:
                        q_objects_compare[order] = Q(
                            **{(order_attr + "__lt"): position}
                        )
                    else:
                        q_objects_compare[order] = Q(
                            **{(order_attr + "__gt"): position}
                        )

                filter_list = [q_objects_compare[self.ordering[0]]]

                ordering = self.ordering

                # starting with the second field
                for i in range(len(ordering)):
                    # The first operands need to be equals
                    # the last operands need to be gt
                    equals = list(ordering[: i + 2])
                    greater_than_q = q_objects_compare[equals.pop()]
                    sub_filters = [q_objects_equals[e] for e in equals]
                    sub_filters.append(greater_than_q)
                    filter_list.append(reduce(operator.and_, sub_filters))

                q_object = reduce(operator.or_, filter_list)
                queryset = queryset.filter(q_object)

        # If we have an offset cursor then offset the entire page by that amount.
        # We also always fetch an extra item in order to determine if there is a
        # page following on from this one.
        results = list(queryset[offset : offset + self.page_size + 1])
        self.page = list(results[: self.page_size])

        # Determine the position of the final item following the page.
        if len(results) > len(self.page):
            has_following_position = True
            following_position = self._get_position_from_instance(
                results[-1], self.ordering
            )
        else:
            has_following_position = False
            following_position = None

        if reverse:
            # If we have a reverse queryset, then the query ordering was in reverse
            # so we need to reverse the items again before returning them to the user.
            self.page = list(reversed(self.page))

            # Determine next and previous positions for reverse cursors.
            self.has_next = (current_position is not None) or (offset > 0)
            self.has_previous = has_following_position
            if self.has_next:
                self.next_position = current_position
            if self.has_previous:
                self.previous_position = following_position
        else:
            # Determine next and previous positions for forward cursors.
            self.has_next = has_following_position
            self.has_previous = (current_position is not None) or (offset > 0)
            if self.has_next:
                self.next_position = following_position
            if self.has_previous:
                self.previous_position = current_position

        # Display page controls in the browsable API if there is more
        # than one page.
        if (self.has_previous or self.has_next) and self.template is not None:
            self.display_page_controls = True

        return self.page

    def get_ordering(self, request, queryset, view):
        """
        Return a tuple of strings, that may be used in an `order_by` method.
        """
        ordering_filters = [
            filter_cls
            for filter_cls in getattr(view, "filter_backends", [])
            if hasattr(filter_cls, "get_ordering")
        ]

        if ordering_filters:
            # If a filter exists on the view that implements `get_ordering`
            # then we defer to that filter to determine the ordering.
            filter_cls = ordering_filters[0]
            filter_instance = filter_cls()
            ordering = filter_instance.get_ordering(request, queryset, view)
            assert ordering is not None, (
                "Using cursor pagination, but filter class {filter_cls} "
                "returned a `None` ordering.".format(filter_cls=filter_cls.__name__)
            )
        else:
            # The default case is to check for an `ordering` attribute
            # on this pagination instance.
            ordering = self.ordering
            assert ordering is not None, (
                "Using cursor pagination, but no ordering attribute was declared "
                "on the pagination class."
            )
            assert "__" not in ordering, (
                "Cursor pagination does not support double underscore lookups "
                "for orderings. Orderings should be an unchanging, unique or "
                'nearly-unique field on the model, such as "-created" or "pk".'
            )

        assert isinstance(ordering, (str, list, tuple)), (
            "Invalid ordering. Expected string or tuple, but got {type}".format(
                type=type(ordering).__name__
            )
        )

        if isinstance(ordering, str):
            ordering = (ordering,)

        pk_name = queryset.model._meta.pk.name

        # Always include a unique key to order by
        if not {"-{}".format(pk_name), pk_name, "pk", "-pk"} & set(ordering):
            ordering = tuple(ordering) + (pk_name,)

        return tuple(ordering)

    def decode_cursor(self, request):
        """
        Given a request with a cursor, return a `Cursor` instance.
        """
        # Determine if we have a cursor, and if so then decode it.
        encoded = request.query_params.get(self.cursor_query_param)
        if encoded is None:
            return None

        try:
            querystring = b64decode(encoded.encode("ascii")).decode("ascii")
            tokens = parse.parse_qs(querystring, keep_blank_values=True)

            offset = tokens.get("o", ["0"])[0]
            offset = _positive_int(offset, cutoff=self.offset_cutoff)

            reverse = tokens.get("r", ["0"])[0]
            reverse = bool(int(reverse))

            position = tokens.get("p", [None])[0]
        except (TypeError, ValueError):
            raise NotFound(self.invalid_cursor_message)

        return Cursor(offset=offset, reverse=reverse, position=position)

    def encode_cursor(self, cursor):
        """
        Given a Cursor instance, return an url with encoded cursor.
        """
        tokens = {}
        if cursor.offset != 0:
            tokens["o"] = str(cursor.offset)
        if cursor.reverse:
            tokens["r"] = "1"
        if cursor.position is not None:
            tokens["p"] = cursor.position

        querystring = parse.urlencode(tokens, doseq=True)
        encoded = b64encode(querystring.encode("ascii")).decode("ascii")
        return replace_query_param(self.base_url, self.cursor_query_param, encoded)

    def _get_position_from_instance(self, instance, ordering):
        fields = []

        for o in ordering:
            field_name = o.lstrip("-")
            if isinstance(instance, dict):
                attr = instance[field_name]
            else:
                attr = getattr(instance, field_name)

            fields.append(str(attr))

        return json.dumps(fields)

    def _all_ascending(self):
        directions = [o.startswith("-") for o in self.ordering]
        return not any(directions)

    def _all_descending(self):
        directions = [o.startswith("-") for o in self.ordering]
        return all(directions)

    def _uniform_ordering(self):
        return self._all_ascending() or self._all_descending()

    def should_use_tuple_comparison(self):
        """Should we use tuple comparison for the cursor?

        - needs all the fields to be ascending or descending
        - needs to be explicitly enabled

        This is only tested with Postgres and SQLite drivers.
        """
        if not self.use_tuple_comparison:
            return False
        if not self._uniform_ordering():
            return False
        return True


class MultiFieldCursorPagination(MultiFieldCursorMixin, CursorPagination):
    pass
