## The essential idea

The core idea is to get news and filter it down. Some filtering algorithms and relevance score determination methods will need to be employed. Plus, I'd not like to read the entire article, but the main essence of it. So, some algorithms are needed there too.

## Design

First step is to get news. For now, I think the best way would be to have access to the RSS feeds, parse them and fetch the news. Major issues that I see with that is the adverts and the pop-ups that the news site may ask me to unblock, or my bot to understand. For this, let's try the 'feedparser'.

### Item object
Let's call a unit of reading an `Article` object. The Article object has various attributes that are either known apriori, or are learned from the data itself. For example, while reading the news feed, some feeds provide a basic category tag where the website has classified that Article. Second, it may contain keywords in its title, summary, content etc. which will need to be extracted from the data and their relevance decided. We are not really interested in the actual content right now, at least from the saving perspective initially. What we want to do is look into the text and decide if the human would want to read it, correlate the information from various sources to see how 'global' it might be.

An article may be classified into various paradigms and we don't want to call each one an attribute, but club all of them into a generic field: category.

Article:
- source URI
- Date Time
- explicit keywords
- extracted keywords
- inferred categories (ranked by score)
- relevance of article
- similar articles
- raw data

A Feedburner Feed Entry has the following entries:
-summary_detail
-storyimage
-published_parsed
-links
-title
-fullimage
-published
-summary
-guidislink
-title_detail
-link
-updatedat
-feedburner_origlink
-id
-tags

Once you have the URI, ask splash to have it parsed (we won't need scrapy here I think, because we are not explicitly crawling, but systematically exploring), and return the data to us. For that, we'll need `requests` module.