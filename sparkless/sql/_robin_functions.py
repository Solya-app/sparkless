"""
Phase 3: Robin-backed functions module.

Re-exports PyO3 functions from sparkless_robin with *args wrappers for
concat, coalesce, format_string, greatest, least, etc.
"""

from __future__ import annotations

from typing import Any


def _get_robin() -> Any:
    try:
        import sparkless_robin as _r  # type: ignore[import-untyped]
        return _r
    except ImportError as e:
        raise ImportError(
            "sparkless_robin native extension is not available. "
            "Build with: maturin develop"
        ) from e


def _robin_functions_module() -> Any:
    """Lazy module-like object that exposes Robin functions with *args wrappers."""
    _r = _get_robin()

    # Optional: first, rank (expose if crate has them)
    _first = getattr(_r, "first", None)
    _rank = getattr(_r, "rank", None)
    _cast = getattr(_r, "cast", None)
    _get_item = getattr(_r, "get_item", None)
    _is_null = getattr(_r, "is_null", None)
    _with_field = getattr(_r, "with_field", None)
    _rlike = getattr(_r, "rlike", None)
    _fill = getattr(_r, "fill", None)
    _over = getattr(_r, "over", None)
    _isin = getattr(_r, "isin", None)
    # Window functions (expose from crate or stub)
    _row_number = getattr(_r, "row_number", None)
    _percent_rank = getattr(_r, "percent_rank", None)
    _lag = getattr(_r, "lag", None)
    _lead = getattr(_r, "lead", None)
    _ntile = getattr(_r, "ntile", None)
    _cume_dist = getattr(_r, "cume_dist", None)
    _dense_rank = getattr(_r, "dense_rank", None)

    from ._robin_column import RobinColumn, _unwrap

    def _wrap_col(col: Any) -> Any:
        return RobinColumn(col) if col is not None else None

    def _agg_alias_name(col: Any, agg: str) -> str:
        """PySpark-style alias for agg(col), e.g. avg(Value), count(Value)."""
        if isinstance(col, str):
            return f"{agg}({col})"
        name = getattr(col, "name", None)
        if isinstance(name, str) and name and "(" not in name and " " not in name:
            return f"{agg}({name})"
        if agg == "count":
            return "count"
        return f"{agg}(col)"

    def _wrap1(f: Any) -> Any:
        """Wrap f so first arg is unwrapped and result is wrapped."""
        def _w(col: Any, *args: Any, **kwargs: Any) -> Any:
            return _wrap_col(f(_unwrap(col), *args, **kwargs))
        return _w

    def _col(name: str) -> Any:
        return _wrap_col(_r.col(name))

    def _lit(value: Any) -> Any:
        return _wrap_col(_r.lit(value))

    def _concat_w(*cols: Any) -> Any:
        return _wrap_col(_r.concat([_unwrap(c) for c in cols]))

    def _concat_ws_w(separator: str, *cols: Any) -> Any:
        return _wrap_col(_r.concat_ws(separator, [_unwrap(c) for c in cols]))

    def _format_string_w(fmt: str, *cols: Any) -> Any:
        return _wrap_col(_r.format_string(fmt, [_unwrap(c) for c in cols]))

    def _coalesce_w(*cols: Any) -> Any:
        return _wrap_col(_r.coalesce([_unwrap(c) for c in cols]))

    def _greatest_w(*cols: Any) -> Any:
        return _wrap_col(_r.greatest([_unwrap(c) for c in cols]))

    def _least_w(*cols: Any) -> Any:
        return _wrap_col(_r.least([_unwrap(c) for c in cols]))

    def _sum_w(col: Any, *args: Any) -> Any:
        """Sum: use first column only so Rust sum(col) is not given extra args."""
        return _wrap_col(_r.sum(_unwrap(col))).alias(_agg_alias_name(col, "sum"))

    def _math_stub(name: str) -> Any:
        def _stub(*args: Any, **kwargs: Any) -> Any:
            raise NotImplementedError(
                f"{name} is not implemented for the Robin backend. "
                "See docs/robin_parity_matrix.md and tests/robin_skip_list.json."
            )
        return _stub

    # Build a namespace with all Robin functions
    class RobinFunctions:
        # Column (wrap so F.col/F.lit return RobinColumn)
        col = staticmethod(_col)
        lit = staticmethod(_lit)

        # String (variadic)
        concat = staticmethod(_concat_w)
        concat_ws = staticmethod(_concat_ws_w)
        format_string = staticmethod(_format_string_w)

        def _array_w(*cols: Any) -> Any:
            return _wrap_col(_r.array([_unwrap(c) for c in cols]))

        def _create_map_w(*key_values: Any) -> Any:
            return _wrap_col(_r.create_map([_unwrap(c) for c in key_values]))

        array = staticmethod(_array_w)
        create_map = staticmethod(_create_map_w)

        # String (single col) - unwrap so crate receives PyColumn
        upper = staticmethod(_wrap1(_r.upper))
        lower = staticmethod(_wrap1(_r.lower))
        trim = staticmethod(_wrap1(_r.trim))
        def _substring_w(col: Any, start: int, length: Any = None) -> Any:
            return _wrap_col(_r.substring(_unwrap(col), start, length))
        substring = staticmethod(_substring_w)
        length = staticmethod(_wrap1(_r.length))
        regexp_extract = staticmethod(_wrap1(_r.regexp_extract))
        regexp_replace = staticmethod(_wrap1(_r.regexp_replace))
        split = staticmethod(_wrap1(_r.split))
        lpad = staticmethod(_wrap1(_r.lpad))
        rpad = staticmethod(_wrap1(_r.rpad))
        contains = staticmethod(_wrap1(_r.contains))
        like = staticmethod(_wrap1(_r.like))

        # Math
        abs = staticmethod(_wrap1(_r.abs))
        ceil = staticmethod(_wrap1(_r.ceil))
        floor = staticmethod(_wrap1(_r.floor))
        round = staticmethod(_wrap1(_r.round))
        sqrt = staticmethod(_wrap1(_r.sqrt))
        pow = staticmethod(_wrap1(_r.pow))
        power = staticmethod(_wrap1(_r.power))
        exp = staticmethod(_wrap1(_r.exp))
        log = staticmethod(_wrap1(_r.log))
        # Trig (expose from crate or stub)
        _sin = getattr(_r, "sin", None)
        _cos = getattr(_r, "cos", None)
        _tan = getattr(_r, "tan", None)
        sin = staticmethod(_wrap1(_sin)) if _sin is not None else staticmethod(_math_stub("sin"))
        cos = staticmethod(_wrap1(_cos)) if _cos is not None else staticmethod(_math_stub("cos"))
        tan = staticmethod(_wrap1(_tan)) if _tan is not None else staticmethod(_math_stub("tan"))

        # Datetime
        year = staticmethod(_wrap1(_r.year))
        month = staticmethod(_wrap1(_r.month))
        day = staticmethod(_wrap1(_r.day))
        hour = staticmethod(_wrap1(_r.hour))
        minute = staticmethod(_wrap1(_r.minute))
        second = staticmethod(_wrap1(_r.second))
        to_date = staticmethod(_wrap1(_r.to_date))
        date_format = staticmethod(_wrap1(_r.date_format))
        current_timestamp = staticmethod(lambda: _wrap_col(_r.current_timestamp()))
        date_add = staticmethod(_wrap1(_r.date_add))
        date_sub = staticmethod(_wrap1(_r.date_sub))

        def _to_timestamp_stub(*args: Any, **kwargs: Any) -> Any:
            raise NotImplementedError(
                "to_timestamp is not implemented for the Robin backend. "
                "Use skip list for tests that require to_timestamp."
            )

        _to_ts = getattr(_r, "to_timestamp", None)
        to_timestamp = staticmethod(
            _wrap1(_to_ts) if _to_ts is not None else _to_timestamp_stub
        )

        # Conditional (variadic)
        coalesce = staticmethod(_coalesce_w)
        greatest = staticmethod(_greatest_w)
        least = staticmethod(_least_w)

        def _when_w(cond: Any, value: Any = None) -> Any:
            """F.when(cond) or F.when(cond, value). Returns builder for .when()/.otherwise() chain or single column."""
            from ._robin_column import RobinCaseWhenBuilder
            if value is None:
                return RobinCaseWhenBuilder(cond)
            return _wrap_col(_r.when_otherwise(_unwrap(cond), _unwrap(value), _r.lit(None)))

        when = staticmethod(_when_w)
        when_otherwise = staticmethod(_r.when_otherwise)

        def _avg_w(col: Any) -> Any:
            return _wrap_col(_r.avg(_unwrap(col))).alias(_agg_alias_name(col, "avg"))

        def _count_w(col: Any) -> Any:
            return _wrap_col(_r.count(_unwrap(col))).alias(_agg_alias_name(col, "count"))

        def _mean_w(col: Any) -> Any:
            return _wrap_col(_r.mean(_unwrap(col))).alias(_agg_alias_name(col, "avg"))

        # Aggregation (sum: single-arg only so Rust sum(col) is not given 2 args)
        sum_ = staticmethod(_sum_w)
        count = staticmethod(_count_w)
        avg = staticmethod(_avg_w)
        mean = staticmethod(_mean_w)

        def _min_w(col: Any, *args: Any) -> Any:
            result = _wrap_col(_r.min(_unwrap(col)))
            if args and args[0] is not None and isinstance(args[0], str):
                return result.alias(args[0])
            return result.alias(_agg_alias_name(col, "min"))

        def _max_w(col: Any, *args: Any) -> Any:
            result = _wrap_col(_r.max(_unwrap(col)))
            if args and args[0] is not None and isinstance(args[0], str):
                return result.alias(args[0])
            return result.alias(_agg_alias_name(col, "max"))

        min_ = staticmethod(_min_w)
        max_ = staticmethod(_max_w)

        def _count_distinct_w(col: Any, *args: Any) -> Any:
            result = _wrap_col(_r.count_distinct(_unwrap(col)))
            if args and args[0] is not None and isinstance(args[0], str):
                return result.alias(args[0])
            return result

        count_distinct = staticmethod(_count_distinct_w)

    # PySpark uses sum, min, max (not sum_, min_, max_)
    RobinFunctions.sum = RobinFunctions.sum_  # type: ignore[attr-defined]
    RobinFunctions.min = RobinFunctions.min_  # type: ignore[attr-defined]
    RobinFunctions.max = RobinFunctions.max_  # type: ignore[attr-defined]
    # PySpark uses countDistinct (camelCase) as well as count_distinct
    RobinFunctions.countDistinct = RobinFunctions.count_distinct  # type: ignore[attr-defined]

    # Stub factory for F.* not in crate (used below and for optional attrs)
    def _not_impl_stub(name: str) -> Any:
        def _stub(*args: Any, **kwargs: Any) -> Any:
            raise NotImplementedError(
                f"{name} is not implemented for the Robin backend. "
                "See docs/robin_parity_matrix.md and tests/robin_skip_list.json."
            )
        return _stub

    # Optional: first, rank (if crate exposes them). Wrap first so F.first(col).over(window) returns RobinColumn.
    if _first is not None:
        def _first_w(col: Any, *args: Any, **kwargs: Any) -> Any:
            return _wrap_col(_first(_unwrap(col), *args, **kwargs))
        RobinFunctions.first = staticmethod(_first_w)  # type: ignore[attr-defined]
    if _rank is not None:
        RobinFunctions.rank = staticmethod(_wrap1(_rank))  # type: ignore[attr-defined]
    def _data_type_to_str(dt: Any) -> str:
        """Convert DataType to crate type string (e.g. IntegerType() -> 'int')."""
        if isinstance(dt, str):
            return dt
        if getattr(dt, "typeName", None) and callable(dt.typeName):
            return dt.typeName()
        if getattr(dt, "simpleString", None) and callable(dt.simpleString):
            return dt.simpleString()
        return str(dt)

    def _cast_w(col: Any, data_type: Any) -> Any:
        """F.cast(col, type): convert DataType to string and call crate cast or col.cast()."""
        type_str = _data_type_to_str(data_type)
        if _cast is not None:
            return _wrap_col(_cast(_unwrap(col), type_str))
        # No top-level cast in crate; use column's cast method
        return _wrap_col(_unwrap(col).cast(type_str))

    RobinFunctions.cast = staticmethod(_cast_w)  # type: ignore[attr-defined]
    # Sort order (col.desc(), col.asc(), etc.) - from Rust module or Column method fallback
    def _sort_order_fallback(method_name: str) -> Any:
        """F.desc_nulls_last(col) via col.desc_nulls_last() when crate has no top-level fn."""

        def _f(col: Any) -> Any:
            if isinstance(col, str):
                c = _col(col)
            else:
                c = col if isinstance(col, RobinColumn) else _wrap_col(col)
            return getattr(c, method_name)()

        return _f

    for _name in ("desc", "asc", "desc_nulls_last", "asc_nulls_last", "desc_nulls_first", "asc_nulls_first"):
        _fn = getattr(_r, _name, None)
        if _fn is not None:
            setattr(RobinFunctions, _name, staticmethod(_fn))  # type: ignore[attr-defined]
        else:
            setattr(RobinFunctions, _name, staticmethod(_sort_order_fallback(_name)))  # type: ignore[attr-defined]
    if _get_item is not None:
        def _get_item_w(col: Any, key: Any, *args: Any, **kwargs: Any) -> Any:
            return _wrap_col(_get_item(_unwrap(col), key, *args, **kwargs))
        RobinFunctions.get_item = staticmethod(_get_item_w)  # type: ignore[attr-defined]
    if _is_null is not None:
        RobinFunctions.is_null = staticmethod(_wrap1(_is_null))  # type: ignore[attr-defined]
    if _with_field is not None:
        def _with_field_w(col: Any, name: Any, value: Any, *args: Any, **kwargs: Any) -> Any:
            val = _unwrap(value) if hasattr(value, "_inner") else value
            return _wrap_col(_with_field(_unwrap(col), name, val, *args, **kwargs))
        RobinFunctions.with_field = staticmethod(_with_field_w)  # type: ignore[attr-defined]
    else:
        RobinFunctions.with_field = staticmethod(_not_impl_stub("with_field"))  # type: ignore[attr-defined]
    if _rlike is not None:
        RobinFunctions.rlike = staticmethod(_wrap1(_rlike))  # type: ignore[attr-defined]
    else:
        RobinFunctions.rlike = staticmethod(_not_impl_stub("rlike"))  # type: ignore[attr-defined]
    if _fill is not None:
        RobinFunctions.fill = staticmethod(_fill)  # type: ignore[attr-defined]
    if _over is not None:
        def _over_w(col: Any, window: Any) -> Any:
            return _wrap_col(_over(_unwrap(col), window))
        RobinFunctions.over = staticmethod(_over_w)  # type: ignore[attr-defined]
    else:
        def _over_stub(col: Any, window: Any) -> Any:
            raise NotImplementedError(
                "over() is not implemented for the Robin backend. "
                "See docs/robin_parity_matrix.md and tests/robin_skip_list.json."
            )
        RobinFunctions.over = staticmethod(_over_stub)  # type: ignore[attr-defined]
    if _isin is not None:
        RobinFunctions.isin = staticmethod(_isin)  # type: ignore[attr-defined]

    # Stubs for F.* not yet in crate (expr, struct, explode, etc.) so F.expr etc. exist
    for _fn_name in (
        "expr", "struct", "explode", "posexplode", "isnan", "array_distinct",
        "approx_count_distinct", "stddev", "stddev_pop", "stddev_samp", "variance",
        "array_contains", "posexplode_outer", "explode_outer", "date_trunc", "input_file_name",
        "size", "element_at", "nvl", "nanvl", "nullif", "last", "first",
        "initcap", "reverse", "repeat", "rtrim", "ltrim", "ascii", "hex", "base64",
        "soundex", "translate", "levenshtein", "crc32", "xxhash64", "get_json_object",
        "json_tuple", "regexp_extract_all", "substring_index", "dayofmonth", "dayofweek",
    ):
        if not hasattr(RobinFunctions, _fn_name):
            setattr(RobinFunctions, _fn_name, staticmethod(_not_impl_stub(_fn_name)))  # type: ignore[attr-defined]

    # Window functions: expose from crate if present, else stub (so F.row_number etc. exist)
    def _window_stub(name: str) -> Any:
        raise NotImplementedError(
            f"{name} is not implemented for the Robin backend. "
            "See docs/robin_parity_matrix.md and tests/robin_skip_list.json."
        )

    def _window_wrap(fn: Any) -> Any:
        """Wrap crate window fn so result is RobinColumn; pass through args for row_number, ntile, etc."""

        def _w(*args: Any, **kwargs: Any) -> Any:
            unwrapped = [_unwrap(a) for a in args]
            return _wrap_col(fn(*unwrapped, **kwargs))

        return _w

    def _lag_w(col: Any, offset: int = 1, default: Any = None) -> Any:
        """F.lag(col, offset=1, default=None). Crate may only accept (col); pass col only."""
        try:
            return _wrap_col(_lag(_unwrap(col), offset, default))
        except TypeError:
            try:
                return _wrap_col(_lag(_unwrap(col), offset))
            except TypeError:
                return _wrap_col(_lag(_unwrap(col)))

    def _lead_w(col: Any, offset: int = 1, default: Any = None) -> Any:
        """F.lead(col, offset=1, default=None). Crate may only accept (col); pass col only."""
        try:
            return _wrap_col(_lead(_unwrap(col), offset, default))
        except TypeError:
            try:
                return _wrap_col(_lead(_unwrap(col), offset))
            except TypeError:
                return _wrap_col(_lead(_unwrap(col)))

    for _wname, _wfn in (
        ("row_number", _row_number),
        ("percent_rank", _percent_rank),
        ("ntile", _ntile),
        ("cume_dist", _cume_dist),
        ("dense_rank", _dense_rank),
    ):
        if _wfn is not None:
            setattr(RobinFunctions, _wname, staticmethod(_window_wrap(_wfn)))  # type: ignore[attr-defined]
        else:
            setattr(
                RobinFunctions,
                _wname,
                staticmethod(lambda n=_wname: _window_stub(n)),  # type: ignore[attr-defined]
            )

    if _lag is not None:
        RobinFunctions.lag = staticmethod(_lag_w)  # type: ignore[attr-defined]
    else:
        RobinFunctions.lag = staticmethod(lambda: _window_stub("lag"))  # type: ignore[attr-defined]
    if _lead is not None:
        RobinFunctions.lead = staticmethod(_lead_w)  # type: ignore[attr-defined]
    else:
        RobinFunctions.lead = staticmethod(lambda: _window_stub("lead"))  # type: ignore[attr-defined]

    return RobinFunctions()


# Singleton
_robin_fns: Any = None


def get_robin_functions() -> Any:
    global _robin_fns
    if _robin_fns is None:
        _robin_fns = _robin_functions_module()
    return _robin_fns
