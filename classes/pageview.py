class Pageview(object):
    __slots__ = [
        "viewtime",
        "userid",
        "pageid"
    ]

    @staticmethod
    def get_schema():
        with open('./avro/pageview.avsc', 'r') as handle:
            return handle.read()
    
    def __init__(self, viewtime, userid, pageid):
        self.viewtime = viewtime
        self.userid   = userid
        self.pageid   = pageid

    @staticmethod
    def dict_to_pageview(obj, ctx=None):
        return Pageview(
                obj['viewtime'],
                obj['userid'],
                obj['pageid']  
            )

    @staticmethod
    def pageview_to_dict(pageview, ctx=None):
        return Pageview.to_dict(pageview)

    def to_dict(self):
        return dict(
                    viewtime = self.viewtime,
                    userid   = self.userid,
                    pageid   = self.pageid
                )