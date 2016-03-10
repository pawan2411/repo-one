
from pymongo import MongoClient
from geo_dist import geodistance

def save_file(file_name):
    
    client = MongoClient('localhost', 27017)
    db = client.test_database  # use a database called "test_database"
    collection = db.weather_files   # and inside that DB, a collection called "weather_files"
    
    root="/Users/z001lbr/Desktop/weather/cleaned/"
    f_name=str(file_name[0])
    f = open(root+f_name)  # open a file
    text = f.read()    # read the entire contents, should be UTF-8 text
    
    # build a document to be inserted

    print(root+f_name)
    text_file_doc = {"file_name":root+f_name , "contents" : text }
    # insert the contents into the "file" collection
    collection.insert(text_file_doc)

#
#if __name__ == "__main__":
#    file_to_be_inserted=(sys.argv[1])
#
#    save_file(file_to_be_inserted)


save_file(geodistance(-93.144848,45.056743))