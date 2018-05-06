# import libraries
from pyspark.mllib.recommendation import *
import random
from operator import *
from pyspark import SparkContext
sc = SparkContext()

# load raw data
rawUserArtistData = sc.textFile("hdfs:/user_artist_data.txt")
rawArtistData = sc.textFile("hdfs:/artist_data_new.txt")
rawArtistAlias = sc.textFile("hdfs:/artist_alias_new.txt")

def getArtistByID(line):
    try:
        (artistID, name) = line.split('\t',1)
        artistID = int(artistID)
    except:
        return []
    return [(artistID, name.strip())]

artistByID = rawArtistData.flatMap(getArtistByID)

artistAlias = rawArtistAlias.map(lambda x: x.split('\t',1)).\
    map(lambda y: (int(y[0]), int(y[1]))).\
    collectAsMap()

bArtistAlias = sc.broadcast(artistAlias)

# generateRating
def generateRating(line):
    (userID, artistID, count) = line.split(' ')
    userID = int(userID)
    artistID = int(artistID)
    count = int(count)
    finalArtistID = bArtistAlias.value.get(artistID)
    if not finalArtistID:
        finalArtistID = artistID
    finalArtistID = int(finalArtistID)
    return Rating(userID, finalArtistID, count)
    
trainData = rawUserArtistData.map(generateRating).cache()

# use defalut model from the book
model = ALS.trainImplicit(trainData, rank=20, iterations=5, lambda_=1.0, alpha=40.0)

# recommendProducts, code from 
# https://spark.apache.org/docs/latest/api/python/_modules/pyspark/mllib/recommendation.html
def recommendProducts(self, user, num):
    """
    Recommends the top "num" number of products for a given user and
    returns a list of Rating objects sorted by the predicted rating in
    descending order.
    """
    return list(self.call("recommendProducts", user, num))

recommendations = recommendProducts(model, 2093760, 10)

recommendedProductIDs = map(lambda (userID, productID, count): productID, recommendations)

def getRecommendedIDs(line):
    (artistID, name) = line
    return artistID in recommendedProductIDs

recommendedProducts = artistByID.filter(getRecommendedIDs).values().collect()
for i in recommendedProducts: print i