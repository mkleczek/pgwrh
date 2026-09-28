def test_key_preserves_full_date_bigint_order(node):
    node.execute("""
        CREATE FUNCTION transaction_key(date, bigint) RETURNS numeric
        LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
        RETURN (CASE WHEN $1 = '-infinity'::date THEN -2147483648::numeric
                     WHEN $1 = 'infinity'::date THEN 2147483647::numeric
                     ELSE ($1 - DATE '2000-01-01')::numeric END)
               * 18446744073709551616::numeric + $2::numeric;
        CREATE TABLE items(d date, id bigint);
        INSERT INTO items SELECT d, id FROM unnest(ARRAY[
          '-infinity'::date, '4713-01-01 BC', '1999-12-31', '2000-01-01',
          '2000-01-02', '2100-12-31', '5874897-12-31', 'infinity']) d
        CROSS JOIN unnest(ARRAY['-9223372036854775808'::bigint,
          -9007199254740993, -1, 0, 1, 9007199254740993, 9223372036854775807]) id;
        CREATE INDEX ON items USING gist(transaction_key(d,id));
        SET enable_seqscan=off;
        SET enable_bitmapscan=off;
    """)
    expected = node.execute('SELECT d::text AS day, id FROM items ORDER BY d,id')
    assert node.execute('SELECT d::text, id FROM items ORDER BY transaction_key(d,id)') == expected
    assert node.execute('SELECT count(*), count(DISTINCT transaction_key(d,id)) FROM items') == [(56, 56)]
    # Page across repeated dates and both ID extremes with an indexable scalar bound.
    after = None
    rows = []
    while True:
        where = '' if after is None else (
            f"WHERE transaction_key(d,id) > transaction_key('{after[0]}', ({after[1]})::bigint)")
        page = node.execute('SELECT d::text AS day,id FROM items ' + where + ' ORDER BY d,id LIMIT 5')
        if not page:
            break
        rows.extend(page)
        after = page[-1]
    assert rows == expected
