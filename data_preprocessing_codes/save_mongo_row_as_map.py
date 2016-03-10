from pymongo import MongoClient
from collections import OrderedDict
from geo_dist import geodistance
from parallel_test_edited_hour import execute

path='/Users/z001lbr/Desktop/a/cleaned/'
fl='processed_file.txt'
fl1='processed_consolidated_file.csv'

#save_file function is reading a txt file, replacing all M by 0s, taking specific columns from all weather parameters, as it is a txt file not a csv first we made map of each value with header as a key then took only values for specific headers and save in a csv format
def save_file(file_name):
    
    file=file_name[0]
    file_nm=path+file

    f= open(file_nm,'r')
    f1=open(path+fl,'w+')
  
    for line in f:
       f1.write(line.replace("M","0"))
       

    f1.close()
    f3= open(path+fl,'r')
    f2=open(path+fl1,'w+')
    i=0
    header_dict = []
    my_list_head = []
    

    for line in f3:
        if i==0:
            my_list_head = line.strip().split(',')
            for k in my_list_head:
                if k=="valid" or k=="tmpf" or k==" dwpf" or k==" relh" or k==" alti" or k==" p01i" or k==" sknt" or k==" vsby":
                    header_dict.append(k)

            i+=1
            break
        
    f3.close()
    f2.write('%s\n'%','.join(header_dict))
    f3= open(path+fl,'r')
    i=0
    for line in f3:
        my_list_data = []
        text_file = OrderedDict()
        if i==0:
            i+=1
            pass
        else:
            my_list_data = line.strip().split(',')
            i=0
            for k in my_list_head:
                j=0
                for key in my_list_data:
                    if i==j:
                        text_file[k]=key
                        
                        i+=1
                        break

                    else:
                        j+=1
                        pass

            consolidated_list = []
            for k in header_dict:
                for k1,v1 in text_file.items():
                    if k==k1:
                        consolidated_list.append(v1)
    

            f2.write('%s\n'%','.join(consolidated_list))
#hourly_file function call sparallel_test_edited.py file to merge the data at hourly interval

def hourly_file():
    consolidated_file=execute(path,fl1)
    return consolidated_file


#save_hourlyfile_mongo function is saving each line of consolidated file as a map inside mongo

def save_hourlyfile_mongo(consolidated_file):
    try:
        client = MongoClient('localhost', 27017)
        db = client.weather_database  # use a database called "test_database"
        collection = db.weather_data   # and inside that DB, a collection called "weather_files"


        with open(path+consolidated_file) as f:
            i=0
            header_dict = OrderedDict()
            for line in f:
            
                text_file_line = OrderedDict()
                if i==0:
                
                    mylist = line.strip().split(',')
                    for key in mylist:
                        header_dict[key]=[]
            
                    i+=1

                else:
                    my_list = line.strip().split(',')
                    i=0
                    for k,v in header_dict.items():
                        j=0
                        for key in my_list:
                            if i==j:
                                text_file_line[k]=key
                                i+=1
                                break

                            else:
                                j+=1
                                pass

                        collection.save(text_file_line)
        client.close()
    except Exception as e:
        print(e)

save_file(geodistance(-93.144848,45.056743))
file=hourly_file()
save_hourlyfile_mongo(file)








'''


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
'''


