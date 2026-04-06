"""Unit tests for pattern detection on string columns."""

import pytest
from data_profiler.enrichment.patterns import detect_patterns


class TestEmailPattern:
    def test_detects_email(self):
        values = ["user@example.com", "admin@test.org", "a@b.co"] * 10
        patterns, scores = detect_patterns(values)
        assert "email" in patterns
        assert scores["email"] == 1.0

    def test_rejects_non_email(self):
        values = ["hello world", "not-an-email", "123456"] * 10
        patterns, _ = detect_patterns(values)
        assert "email" not in patterns

    def test_mixed_email_below_threshold(self):
        values = ["user@example.com"] + ["plain text"] * 9
        patterns, _ = detect_patterns(values)
        assert "email" not in patterns  # 10% < 50% threshold


class TestPhonePattern:
    def test_detects_us_phone(self):
        values = ["555-123-4567", "(555) 123-4567", "+1 555.123.4567"] * 10
        patterns, scores = detect_patterns(values)
        assert "phone_us" in patterns

    def test_rejects_random_numbers(self):
        values = ["12345", "99", "1000000"] * 10
        patterns, _ = detect_patterns(values)
        assert "phone_us" not in patterns


class TestSSNPattern:
    def test_detects_ssn(self):
        values = ["123-45-6789", "987-65-4321", "111-22-3333"] * 10
        patterns, scores = detect_patterns(values)
        assert "ssn" in patterns

    def test_partial_ssn_still_flags(self):
        # SSN has 0.3 threshold, so 40% should trigger
        values = ["123-45-6789"] * 4 + ["plain text"] * 6
        patterns, _ = detect_patterns(values)
        assert "ssn" in patterns


class TestUUIDPattern:
    def test_detects_uuid(self):
        values = [
            "550e8400-e29b-41d4-a716-446655440000",
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        ] * 10
        patterns, scores = detect_patterns(values)
        assert "uuid" in patterns

    def test_case_insensitive(self):
        values = ["550E8400-E29B-41D4-A716-446655440000"] * 10
        patterns, _ = detect_patterns(values)
        assert "uuid" in patterns


class TestIPv4Pattern:
    def test_detects_ipv4(self):
        values = ["192.168.1.1", "10.0.0.1", "172.16.0.1"] * 10
        patterns, scores = detect_patterns(values)
        assert "ipv4" in patterns

    def test_rejects_partial_ip(self):
        values = ["192.168.1", "10.0", "1.2"] * 10
        patterns, _ = detect_patterns(values)
        assert "ipv4" not in patterns


class TestCreditCardPattern:
    def test_detects_credit_card(self):
        values = ["4111-1111-1111-1111", "5500 0000 0000 0004"] * 10
        patterns, scores = detect_patterns(values)
        assert "credit_card" in patterns

    def test_partial_cards_flag_at_low_threshold(self):
        # Credit card has 0.3 threshold
        values = ["4111-1111-1111-1111"] * 4 + ["other stuff"] * 6
        patterns, _ = detect_patterns(values)
        assert "credit_card" in patterns


class TestURLPattern:
    def test_detects_url(self):
        values = ["https://example.com", "http://test.org/path?q=1"] * 10
        patterns, scores = detect_patterns(values)
        assert "url" in patterns

    def test_rejects_non_url(self):
        values = ["not a url", "ftp://something", "example.com"] * 10
        patterns, _ = detect_patterns(values)
        assert "url" not in patterns


class TestDateStringPattern:
    def test_detects_date_strings(self):
        values = ["2024-01-15", "2023-12-31", "2025-06-01"] * 10
        patterns, scores = detect_patterns(values)
        assert "date_string" in patterns


class TestEdgeCases:
    def test_empty_values(self):
        patterns, scores = detect_patterns([])
        assert patterns == []
        assert scores == {}

    def test_whitespace_trimming(self):
        values = ["  user@example.com  ", " admin@test.org "] * 10
        patterns, _ = detect_patterns(values)
        assert "email" in patterns

    def test_no_patterns_on_random_strings(self):
        values = ["abc", "def", "ghi", "jkl", "mno"] * 10
        patterns, _ = detect_patterns(values)
        assert patterns == []

    def test_multiple_patterns_possible(self):
        # A column could potentially match multiple patterns
        # but in practice our regexes are specific enough that this is rare
        values = ["123-45-6789"] * 10
        patterns, _ = detect_patterns(values)
        assert "ssn" in patterns
