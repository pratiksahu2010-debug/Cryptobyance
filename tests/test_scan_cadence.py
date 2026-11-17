import unittest

import main


class ScannerCadenceTests(unittest.TestCase):
    def test_normal_scan_waits_until_next_utc_aligned_five_minute_close(self):
        # 10 seconds into the 10:00-10:05 UTC candle.
        delay = main.scanner_sleep_seconds(now_epoch=610, stale_data=False)
        self.assertEqual(delay, 293)

    def test_stale_data_waits_for_next_five_minute_close_not_repeated_old_bar(self):
        # 4m10s after a boundary; the next close plus grace is 53s away.
        delay = main.scanner_sleep_seconds(now_epoch=850, stale_data=True)
        self.assertEqual(delay, 53)

    def test_stale_retry_waits_for_imminent_candle_close_plus_grace(self):
        # 25 seconds before the next close, wait for close + 3s grace.
        delay = main.scanner_sleep_seconds(now_epoch=875, stale_data=True)
        self.assertEqual(delay, 28)

    def test_stale_data_waits_for_full_remaining_interval_when_close_is_far(self):
        delay = main.scanner_sleep_seconds(now_epoch=610, stale_data=True)
        self.assertEqual(delay, 293)

    def test_stale_retry_does_not_sleep_past_the_next_close_window(self):
        delay = main.scanner_sleep_seconds(now_epoch=898, stale_data=True)
        self.assertEqual(delay, 5)

    def test_boundary_scan_includes_small_finalization_grace(self):
        delay = main.seconds_until_next_candle_scan(now_epoch=898)
        self.assertEqual(delay, 5)


if __name__ == "__main__":
    unittest.main()
