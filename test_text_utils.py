import unittest

from text_utils import normalize_text


class NormalizeTextTests(unittest.TestCase):
    def test_keeps_valid_hindi_text(self):
        text = "हैदराबाद में ओवैसी ने 5वीं बार दर्ज की जीत"
        self.assertEqual(normalize_text(text), text)

    def test_repairs_common_utf8_latin1_mojibake(self):
        original = "हैदराबाद में ओवैसी ने 5वीं बार दर्ज की जीत"
        broken = original.encode("utf-8").decode("latin1")
        self.assertEqual(normalize_text(broken), original)

    def test_repairs_common_bengali_utf8_latin1_mojibake(self):
        original = "কলকাতায় আজ বৃষ্টি হতে পারে"
        broken = original.encode("utf-8").decode("latin1")
        self.assertEqual(normalize_text(broken), original)

    def test_repairs_mac_roman_mojibake(self):
        original = "हैदराबाद में ओवैसी ने 5वीं बार दर्ज की जीत"
        broken = original.encode("utf-8").decode("mac_roman")
        self.assertEqual(normalize_text(broken), original)

    def test_repairs_mac_roman_mojibake_when_first_marker_is_missing(self):
        original = "हैदराबाद में ओवैसी ने 5वीं बार दर्ज की जीत"
        broken = original.encode("utf-8").decode("mac_roman")[1:]
        self.assertEqual(normalize_text(broken), original)


if __name__ == "__main__":
    unittest.main()
