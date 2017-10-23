import gdata.youtube
import gdata.youtube.service

FEED_URI_PREFIX= 'http://gdata.youtube.com/feeds/api/videos/'
FUP = FEED_URI_PREFIX
def GetAndPrintVideoFeed(uri, yt_obj=None):
    if yt_obj is None:
        yt_obj = gdata.youtube.service.YouTubeService()
    if uri is None:
	raise Exception('URI not provided')
    feed = yt_obj.GetYouTubeVideoFeed(uri)
    for entry in feed.entry:
        PrintEntryDetails(entry)


#GetAndPrintVideoFeed(uri=FUP+'Vkcki2dRCxM')
#yt_service = gdata.youtube.service.YouTubeService()
#entry = yt_service.GetYouTubeVideoEntry(video_id='Vkcki2dRCxM')
#PrintEntryDetails(entry)
def PrintVideoFeed(feed):
    for entry in feed.entry():
        PrintEntryDetails(entry)

def GetAndPrintFeedByUrl():
    yt_service = gdata.youtube.service.YouTubeService()
   
    uri = 'http://gdata.youtube.com/feeds/api/standardfeeds/JP/most_popular'
    PrintVideoFeed(yt_service.GetYouTubeVideoFeed(uri))

GetAndPrintFeedByUrl()
