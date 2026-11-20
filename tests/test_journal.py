import unittest

from journal import FEEDBACK_CATEGORIES, JOURNAL_DDL


class JournalSchemaTests(unittest.TestCase):
    def test_journal_tables_are_additive_and_auditable(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS journal_feedback", JOURNAL_DDL)
        self.assertIn("CREATE TABLE IF NOT EXISTS journal_candidates", JOURNAL_DDL)
        self.assertIn("UNIQUE(symbol,direction,candle_open_ms)", JOURNAL_DDL)
        self.assertIn("ON DELETE RESTRICT", JOURNAL_DDL)

    def test_feedback_categories_include_fake_and_skip(self):
        self.assertIn("FAKE_SIGNAL", FEEDBACK_CATEGORIES)
        self.assertIn("SKIP", FEEDBACK_CATEGORIES)

    def test_feedback_categories_cover_signal_and_missed_setup_review(self):
        self.assertIn("GOOD_ENTRY", FEEDBACK_CATEGORIES)
        self.assertIn("BAD_SIGNAL", FEEDBACK_CATEGORIES)
        self.assertIn("LATE_ENTRY", FEEDBACK_CATEGORIES)
        self.assertIn("MISSED_OPPORTUNITY", FEEDBACK_CATEGORIES)
        self.assertIn("STOP_LOSS_TOO_TIGHT", FEEDBACK_CATEGORIES)
        self.assertIn("TARGET_TOO_CLOSE", FEEDBACK_CATEGORIES)
        self.assertIn("WRONG_DIRECTION", FEEDBACK_CATEGORIES)
        self.assertIn("NEEDS_REVIEW", FEEDBACK_CATEGORIES)


if __name__ == "__main__":
    unittest.main()
