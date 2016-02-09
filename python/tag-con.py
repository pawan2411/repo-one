 # Staging configuration                                                              
 # Keys should be UPPERCASE                                                           
 # http://flask.pocoo.org/docs/0.10/config/                                           
                                                                                      
 RECASER_URL = "http://recaser.factset.io/fundamentals-recaser"                       
 TT_URL = "http://services-staging.factset.com/ccstranslation-tt2"                    
 KOREAN_TOKENIZER_URL = "http://services-staging.factset.com/fundamentals-koreanmorph"
 MOSES_URL = "http://ccs-translation-vip.prod.factset.com:"                           
                                                                                      
 TOKENIZER_DIR = "../tokenizer/"                                                      
                                                                                      
 # Moses language services ports                                                      
 MOSES_PORTS = {                                                                      
     'dan': 5162,                                                                     
     'heb': 5154,                                                                     
     'vie': 5155,                                                                     
     'ind': 5156,                                                                     
     'tha': 5157,                                                                     
     'tur': 5158,                                                                     
     'bul': 5159,                                                                     
     'ron': 5160,                                                                     
     'slk': 5161,                                                                     
     'fin': 5163,                                                                     
     'nor': 5164,                                                                     
     'hun': 5165,                                                                     
     'slv': 5166,                                                                     
     'jpn': 5167,                                                                     
     'kor': 5168,                                                                     
     'zho': 5169,                                                                     
     'isl': 5170,                                                                     
     'mkd': 5171,                                                                     
     'ces': 5172,                                                                     
     'spa': 5173,                                                                     
     'fra': 5174,                                                                     
     'ita': 5175                                                                      
 }                                                                                    