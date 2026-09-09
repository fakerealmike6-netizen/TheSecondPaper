import re
import unittest
from stage1d_bq_sql_guard import sql_code


def allowed(sql):
    code=sql_code(sql).strip()
    return bool(re.match(r'(?:SELECT|WITH)\b',code,re.I)) and ';' not in code and not re.search(
        r'\b(?:INSERT|DELETE|UPDATE|CREATE|DROP|ALTER|MERGE|CALL|EXPORT|EXECUTE|EXTERNAL_QUERY)\b',code,re.I)


class SqlGuardTests(unittest.TestCase):
    def test_native_create_literal_is_read_only(self):
        self.assertTrue(allowed("SELECT CASE WHEN trace_type IN ('create','create2') THEN to_address END FROM `safe.table`"))
    def test_actual_ddl_still_rejected(self):
        self.assertFalse(allowed('CREATE TABLE x AS SELECT 1'))
    def test_with_dml_still_rejected(self):
        self.assertFalse(allowed('WITH x AS (SELECT 1) DELETE FROM y'))
    def test_second_statement_still_rejected(self):
        self.assertFalse(allowed("SELECT 'ok'; DROP TABLE x"))
    def test_external_query_remains_rejected(self):
        self.assertFalse(allowed("SELECT * FROM EXTERNAL_QUERY('x','SELECT 1')"))
    def test_literal_comment_markers_do_not_hide_real_statement(self):
        self.assertFalse(allowed("SELECT '-- harmless'; DELETE FROM x"))
    def test_actual_comments_are_masked(self):
        self.assertTrue(allowed('/* CREATE only documentation */ SELECT 1 -- DELETE text\n'))
    def test_triple_literals_cannot_expose_or_hide_executable_ddl(self):
        self.assertTrue(allowed("SELECT '''create; drop\n-- string'''"))
        self.assertFalse(allowed("SELECT '''literal'''; DROP TABLE x"))
    def test_escaped_literal_ends_before_real_ddl(self):
        self.assertFalse(allowed("SELECT 'escaped\\\' quote'; DELETE FROM x"))
    def test_unterminated_tokens_rejected(self):
        for sql in ("SELECT 'unterminated",'SELECT /* unterminated','SELECT `unterminated'):
            with self.subTest(sql=sql), self.assertRaises(ValueError): sql_code(sql)
    def test_quoted_identifier_does_not_create_ddl(self):
        self.assertTrue(allowed('SELECT `create` FROM `safe.table`'))


if __name__=='__main__': unittest.main()
