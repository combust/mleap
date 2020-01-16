import re
from pandas.testing import assert_frame_equal


def _print_dfs(actual, expected):
    print('actual.show(100):')
    actual.show(100, False)
    print('expected.show(100):')
    expected.show(100, False)


def assert_df(actual, expected, sort=True):
    """
    For more elaborate DFs comparison, e.g. ignoring sorting or less precise.
    Default precision for float comparison is 5 digits.
    """
    __tracebackhide__ = True

    if sort:
        actual = actual.orderBy(actual.columns)
        expected = expected.orderBy(actual.columns)

    try:
        assert_frame_equal(actual.toPandas(), expected.toPandas())
    except AssertionError as e:
        _print_dfs(actual, expected)
        # assert_frame_equal doesn't print the column name, only the index
        #   -> get the index from message with regex & resolve column name
        matcher = re.search(r'iloc\[:, (\d+)\]', e.args[0])
        if matcher:
            iloc = matcher.group(1)
            if iloc is not None:
                raise AssertionError("failed assert on column '{}': {}".format(actual.columns[int(iloc)], e))
        # couldn't extract column name. unexpected.
        raise e
