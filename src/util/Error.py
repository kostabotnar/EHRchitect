class QueryBuilderError(IOError):
    def __int__(self, msg: str):
        self.msg = msg

    def __str__(self):
        return self.__class__.__name__ + ' : ' + self.msg
