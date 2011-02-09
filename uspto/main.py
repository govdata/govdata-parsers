def download_from_dvn():
    pass

def unzip_dvn():
    pass

def reorder():
    pass

def create_metadata():
    """
      the metadata
      it has to define a source and some other things that we'll
      decied on later ...
      it will return a python dictionary whose keys are the fields
      and whose values are the metadata values for those fields
      {"title":"USPTO Data", "keywords":["patent",...], } &c
      
    """
    pass

class uspto_iterator(govdata.core.DataIterator):
    pass

    def __init__(self):
        pass

    def refresh(self):
        """
          open an iterative cursor to the 9 sqlite files
          and load into the metadata attribute the metadata
        """

        #open cursors
        #self.cursors =  dictionary of cursors

        #self.metadata = create_metadata()
        
        pass

    def next(self):

        """
         this actually returns the schema record
        """

        #call the next methods as needed of the 9 cursors
        #some will get called multiple times for each single call
        #to the patent DB
        #transform the data into a record that can be inserted in to MongDB
        #return that value
        
        pass
