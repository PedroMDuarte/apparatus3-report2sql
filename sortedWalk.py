import os

def sortedWalk(top, topdown=True, onerror=None):
   from os.path import join,isdir,islink

   names = os.listdir(top)
   names.sort()
   dirs, nondirs = [], []
 
   for name in names:
      if isdir(os.path.join(top,name)):
         dirs.append(name)
      else:
         nondirs.append(name)

   if topdown:
      yield top, dirs, nondirs
   for name in dirs:
      path = join(top,name)
      if not os.path.islink(path):
         for x in sortedWalk( path, topdown, onerror):
            yield x
   if not topdown:
      yield top, dirs, nondirs

  




  


