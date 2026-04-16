import importlib
import sys
import types
import unittest


db_stub = types.ModuleType("db")
db_stub.generic_collection = object()
sys.modules.setdefault("db", db_stub)

generic_scraper = importlib.import_module("generic_scraper")


class GenericScraperUtilsTests(unittest.TestCase):
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

    def test_is_article_url_rejects_topic_page(self):
        url = "https://www.aajtak.in/topic/elections"
        self.assertFalse(generic_scraper._is_article_url(url, "www.aajtak.in"))


if __name__ == "__main__":
    unittest.main()
