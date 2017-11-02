#This is exploratory
import feedparser
from datetime import datetime
from time import mktime
from  pprint import PrettyPrinter

class Article(object):
  def __init__(self, uri=None, date=None, tags=None, raw_data=None):
    """
    raw_data is a JSON field containing various things that I would like:
    - Summary
    - Title
    - Body
    """
    self.uri = uri
    self.date = date
    self.raw_data = raw_data
    self.explicit_keywords = tags
    self.extracted_keywords = []
    self.categories = []
    self.relevance = None
    self.similar = []

  @classmethod
  def parse_feed_item(self, item, source='FeedBurner'):
     if source == 'FeedBurner':
       raw_data = {}
       raw_data['title'] = item.title
       raw_data['summary'] = item.summary
       article = Article(uri=item.feedburner_origlink,
                         date=datetime.fromtimestamp(mktime(item.published_parsed)),
                         tags=[tag.term for tag in item.tags],
                         raw_data = raw_data)
       return article

  def __str__(self):
    return ('URI:{uri}\nDate: {date}\nTags:{tags}\nData:{data}'.format(uri=self.uri,
                                   date=datetime.strftime(self.date, '%d-%B-%y %H:%M:%S %z'),
                                   tags=','.join(self.explicit_keywords),
                                   data=self.raw_data))



d = feedparser.parse('https://feeds.feedburner.com/NDTV-LatestNews')

#Generic Feed information
#for key, value in d.feed.iteritems():
#  print str(key)+':'+str(value)

#Reading the Feed entries
printer = PrettyPrinter(indent=2)
for entry in d.entries:
  article = Article.parse_feed_item(entry, 'FeedBurner')
  print(article)
  #print type(entry.published_parsed)
  #printer.pprint(entry)
  # for key in entry:
  #   print key
  #   if key=='tags':
  #     print entry[key]
  print '\n'

