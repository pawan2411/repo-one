
def get_text(name):
   return "lorem ipsum, {0} dolor sit amet".format(name)

def p_decorate(func):
   def func_wrapper(name):
       return "<p>{0}</p>".format(func(name))
   return func_wrapper

get_text = p_decorate(get_text)

print get_text("John")

# <p>Outputs lorem ipsum, John dolor sit amet</p>