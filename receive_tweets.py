import tweepy
from tweepy.auth import OAuthHandler
# from tweepy import Stream
from tweepy.streaming import Stream
import socket
import json

# request to get credentials at http://apps.twitter.com
consumer_key = 'aOxHGXjzqKTP9k8DKJRf0ZRDD'
consumer_secret = 'MvzAgwQUNPB5IRf6TcH6CQHPHqgNI3SpL360GmJMHPL8YF1fJo'
access_token    = '1743623826116878336-q49lvWDyL9xwXz5aEqRDtOaq1bccul'
access_secret   = 'UsIfxX7dfGXb57398YniXxkHCeobFviqCxvkTFPAvhRnd'

# we create this class that inherits from the StreamListener in tweepy StreamListener.
class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    # we override the on_data() function in StreamListener.
    def on_data(self, data):
        try:
            message = json.loads(data)
            print(message['text'].encode('utf-8'))
            self.client_socket.send(message['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))

    def if_error(self, status):
        print(status)
        return True

def send_tweets(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = tweepy.Stream(auth, TweetsListener(c_socket))
    # we are interested in this topic.
    twitter_stream.filter(track=['football'])

if __name__ == "__main__":
    
    # initiate a socket object.
    new_skt = socket.socket()
    host = "127.0.0.1"
    port = 5555
    # Binding host and port
    new_skt.bind((host, port))

    print("Now listening on port: %s" % str(port))

    # waiting for client connection.
    new_skt.listen(5)
    # Establish connection with client. it returns first a socket object, c, and the address bound to the socket
    c, addr = new_skt.accept()

    print("Received request from : " + str(addr))
    # and after accepting the connection, we all sent the tweets through the socket
    send_tweets(c)

