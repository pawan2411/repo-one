import re
str="<TItLe> hola </titlE>"
title_rem = re.sub('<(?i)title>(.+?)</title>', '<title>hahah</title>', str)

print title_rem

