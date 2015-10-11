import gdata.youtube
import gdata.youtube.service
from api_key import developer_key, user_info

class InvalidArgs(Exception):
    def __init__(self, message):
        super(InvalidArgs, self).__init__(message)

yt_service = gdata.youtube.service.YouTubeService()

#Turn on HTTPS/SSL: Does not apply to uploads yet.
yt_service.ssl = True
yt_service.email = user_info.get('email', None)
if yt_service.email is None:
    raise InvalidArgs('Invalid email \'None\'')

yt_service.password = user_info.get('password', None)
if yt_service.password is None:
    raise InvalidArgs('Invalid password \'None\'')

yt_service.source = 'Tutorial code'

yt_service.developer_key = developer_key.get('api_key', None)
if yt_service.developer_key is None:
    raise InvalidArgs('Invalid developer key \'None\'')

yt_service.client_id = developer_key.get('client_id', None)
if yt_service.client_id is None:
    raise InvalidArgs('Invalid email \'client_id\'')


#Possible issues with logging in. See http://stackoverflow.com/questions/26614321/reading-a-private-google-spreadsheet-using-a-python-client-library

yt_service.ProgrammaticLogin()

