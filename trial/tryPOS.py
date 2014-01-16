#encoding:utf-8
import nltk as nk
import time
from nltk.corpus import treebank
stime = time.time()
porter = nk.PorterStemmer()

for i in range(1):
    
    sentence = """I love apples. 2NZ is an Australian radio station serving the Inverell, New South Wales|Inverell region. It was opened in January 1937."""
    tokens = nk.word_tokenize(sentence)
    tagged = nk.pos_tag(tokens)
    print tokens
    print tagged[0:]
    for each in tokens:
        print each,porter.stem(each)
etime = time.time()
print etime-stime