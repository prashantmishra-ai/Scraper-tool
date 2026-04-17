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

    def test_frontpage_section_priority_scores_latest_and_trending_labels(self):
        self.assertGreater(generic_scraper._frontpage_section_priority("Latest News"), 0)
        self.assertGreater(generic_scraper._frontpage_section_priority("Top Headlines"), 0)
        self.assertGreater(generic_scraper._frontpage_section_priority("Trending"), 0)
        self.assertEqual(generic_scraper._frontpage_section_priority("Editorial archive"), 0)

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

    def test_is_google_news_url_detects_news_google_domain(self):
        self.assertTrue(generic_scraper._is_google_news_url("https://news.google.com/home?hl=en-IN&gl=IN"))
        self.assertFalse(generic_scraper._is_google_news_url("https://www.aajtak.in/"))

    def test_is_google_news_story_url_detects_google_story_paths(self):
        self.assertTrue(generic_scraper._is_google_news_story_url("https://news.google.com/read/CBMiXEF..."))
        self.assertTrue(generic_scraper._is_google_news_story_url("https://news.google.com/articles/CBMiXEF..."))
        self.assertFalse(generic_scraper._is_google_news_story_url("https://news.google.com/home?hl=en-IN&gl=IN"))

    def test_pick_google_news_story_href_prefers_external_article(self):
        hrefs = [
            "https://news.google.com/read/CBMiXEF123",
            "https://www.thehindu.com/news/national/sample-story/article69432123.ece",
            "https://news.google.com/home?hl=en-IN&gl=IN",
        ]
        picked = generic_scraper._pick_google_news_story_href(hrefs, prefer_external=False)
        self.assertEqual(picked, "https://www.thehindu.com/news/national/sample-story/article69432123.ece")

    def test_pick_google_news_story_href_uses_google_story_when_no_external_article_exists(self):
        hrefs = [
            "https://news.google.com/read/CBMiXEF123",
            "https://news.google.com/home?hl=en-IN&gl=IN",
        ]
        picked = generic_scraper._pick_google_news_story_href(hrefs, prefer_external=False)
        self.assertEqual(picked, "https://news.google.com/read/CBMiXEF123")

    def test_parse_google_news_text_extracts_sections_and_stories(self):
        sample = """
        Your briefing
        Friday, 17 April
        Top stories
        Police: TCS Nashik staff face harassment and conversion claims chevron_right
        The Hindu
        TCS Nashik case: Plea in Supreme Court seeks directions to declare forced religious conversion as terrorist act
        3 hours ago
        The Times of India
        Uncle of accused in TCS Nashik case says scripted conspiracy by Bajrang Dal
        1 hour ago
        Local news
        Hindustan Times
        Registration begins at exams.nta.nic.in direct link to apply here
        18 minutes ago
        By Papri Chanda
        """
        rows = generic_scraper._parse_google_news_text(sample)
        self.assertIn(["Section", "Top stories", ""], rows)
        self.assertIn(
            [
                "Google News Story",
                "TCS Nashik case: Plea in Supreme Court seeks directions to declare forced religious conversion as terrorist act",
                "Top stories | Police: TCS Nashik staff face harassment and conversion claims | The Hindu",
            ],
            rows,
        )
        self.assertIn(
            ["Google News Story Meta", "18 minutes ago | By Papri Chanda", "Registration begins at exams.nta.nic.in direct link to apply here"],
            rows,
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

    def test_is_article_url_accepts_abplive_story(self):
        url = "https://news.abplive.com/news/world/sample-abp-live-story-about-global-conflict-1836991"
        self.assertTrue(generic_scraper._is_article_url(url, "news.abplive.com"))

    def test_is_article_url_rejects_news18_video_page(self):
        url = "https://www.news18.com/videos/world/sample-video-package-9421873.html"
        self.assertFalse(generic_scraper._is_article_url(url, "www.news18.com"))

    def test_is_article_url_rejects_topic_page(self):
        url = "https://www.aajtak.in/topic/elections"
        self.assertFalse(generic_scraper._is_article_url(url, "www.aajtak.in"))


if __name__ == "__main__":
    unittest.main()
