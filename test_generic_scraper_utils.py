import importlib
import sys
import types
import unittest


db_stub = types.ModuleType("db")
db_stub.generic_collection = object()
sys.modules.setdefault("db", db_stub)

generic_scraper = importlib.import_module("generic_scraper")


class GenericScraperUtilsTests(unittest.TestCase):
    def test_normalize_generic_mode_maps_frontpage_aliases(self):
        self.assertEqual(generic_scraper._normalize_generic_mode("frontpage"), "frontpage_news")
        self.assertEqual(generic_scraper._normalize_generic_mode("news"), "frontpage_news")
        self.assertEqual(generic_scraper._normalize_generic_mode("deep"), "deep")

    def test_looks_like_headline_accepts_article_style_text(self):
        self.assertTrue(
            generic_scraper._looks_like_headline("India election update: key cabinet meeting likely today")
        )
        self.assertTrue(
            generic_scraper._looks_like_headline("Trump visit to Islamabad likely if Iran deal is signed")
        )

    def test_looks_like_headline_rejects_ui_copy(self):
        self.assertFalse(generic_scraper._looks_like_headline("Read more"))
        self.assertFalse(generic_scraper._looks_like_headline("Live TV"))

    def test_should_expand_article_control_accepts_hindi_and_english_article_prompts(self):
        self.assertTrue(generic_scraper._should_expand_article_control("Read More"))
        self.assertTrue(generic_scraper._should_expand_article_control("पूरा पढ़ें"))
        self.assertTrue(generic_scraper._should_expand_article_control("अगला पेज"))

    def test_should_expand_article_control_rejects_non_article_prompts(self):
        self.assertFalse(generic_scraper._should_expand_article_control("Share"))
        self.assertFalse(generic_scraper._should_expand_article_control("ये भी पढ़ें"))
        self.assertFalse(generic_scraper._should_expand_article_control("Watch Live TV"))

    def test_canonicalize_url_removes_tracking_params(self):
        url = "https://example.com/world/story/test-123?utm_source=x&fbclid=y&id=7"
        self.assertEqual(
            generic_scraper._canonicalize_url(url),
            "https://example.com/world/story/test-123?id=7",
        )

    def test_extract_embedded_article_data_from_json_ld(self):
        html = """
        <html><head>
        <script type="application/ld+json">
        {
          "@context": "https://schema.org",
          "@type": "NewsArticle",
          "headline": "Example headline",
          "articleBody": "Paragraph one. Paragraph two.",
          "datePublished": "2026-04-16T10:00:00Z",
          "author": {"@type":"Person","name":"Reporter Name"},
          "articleSection": "World"
        }
        </script>
        </head><body></body></html>
        """
        data = generic_scraper._extract_embedded_article_data_from_html(html)
        self.assertEqual(data["headline"], "Example headline")
        self.assertIn("Paragraph one.", data["article_body"])
        self.assertEqual(data["source"], "Reporter Name")
        self.assertEqual(data["category"], "World")

    def test_is_article_url_accepts_nytimes_story(self):
        url = "https://www.nytimes.com/2026/04/15/us/sample-story.html"
        self.assertTrue(generic_scraper._is_article_url(url, "www.nytimes.com"))

    def test_is_article_url_accepts_news18_story(self):
        url = "https://www.news18.com/world/trump-says-he-might-visit-islamabad-if-iran-deal-is-signed-9421873.html"
        self.assertTrue(generic_scraper._is_article_url(url, "www.news18.com"))

    def test_is_article_url_rejects_news18_video_page(self):
        url = "https://www.news18.com/videos/world/sample-video-package-9421873.html"
        self.assertFalse(generic_scraper._is_article_url(url, "www.news18.com"))

    def test_is_article_url_rejects_topic_page(self):
        url = "https://www.aajtak.in/topic/elections"
        self.assertFalse(generic_scraper._is_article_url(url, "www.aajtak.in"))


if __name__ == "__main__":
    unittest.main()
