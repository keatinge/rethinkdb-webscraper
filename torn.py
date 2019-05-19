import tornado.ioloop
import tornado.web
import numpy as np
import tornado.locks
import tornado.queues
import tornado.httpclient
import tornado.websocket
import rethinkdb
import datetime
import pytz

import json

CHROME_UA = "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36"

def now_eastern():
    tz_info = pytz.timezone("US/Eastern")
    return datetime.datetime.now(tz_info)


class BoundedHTTP:
    def __init__(self, concurrent_http_reqs_allowed):
        self.http_sem = tornado.locks.Semaphore(concurrent_http_reqs_allowed)

    async def http_request(self, url):
        await self.http_sem.acquire()
        try:
            print("Requesting", url)
            await tornado.gen.sleep(np.random.normal(loc=1.0, scale=0.2))

        finally:
            self.http_sem.release()

        return "Completed " + url



class LiveScrapeReq:
    def __init__(self, url, after_req_f, live_scrape):
        self.created_at = datetime.datetime.now()
        self.after_req_f = after_req_f
        self.url = url
        self.live_scrape_instance = live_scrape


    async def execute(self):
        # Logging

        self.execute_start = datetime.datetime.now()

        to_insert_data = {
            "url" : self.url,
            "created_at" : now_eastern(),
            "completed_at" : None,
            "raw_response" : None,
            "parsed_data" : None
        }


        scrape_table = self.live_scrape_instance.r.table("new")
        conn = self.live_scrape_instance.conn
        changes = await scrape_table.insert(to_insert_data).run(conn)
        if changes["errors"] != 0:
            raise Exception("Got an error inserting into rethink")

        this_req_uuid = changes["generated_keys"][0]
        print("Requesting", self.url)
        #response = await tornado.gen.sleep(np.random.normal(loc=1.0, scale=0.2))
        response = await tornado.httpclient.AsyncHTTPClient().fetch(self.url, user_agent=CHROME_UA)
        data_to_save = await self.after_req_f(response, self.live_scrape_instance)


        update_data = {
            "completed_at" : now_eastern(),
            #"raw_response" : response.body.decode("utf-8"),
            "parsed_data" : data_to_save
        }

        await scrape_table.get(this_req_uuid).update(update_data).run(conn)
        if data_to_save is not None:
            pass
        print("Done with", self.url)





class LiveScrape:
    def __init__(self, project, num_conc_reqs=3, wipe=False):
        self.project = project
        self.http_sem = tornado.locks.Semaphore(num_conc_reqs)
        self.queue = tornado.queues.Queue(maxsize=0)
        self.loop = tornado.ioloop.IOLoop().current()

        self.r = None
        self.conn = None
        self.loop.run_sync(self.initialize_rethinkdb)

        if wipe:
            self.loop.run_sync(self.wipe_table)


    def enqueue_request(self, url, callback):
        self.queue.put(LiveScrapeReq(url, callback, self))
        self.loop.spawn_callback(self.process_one_from_queue)


    async def wipe_table(self):
        print("Wiping table")
        await self.r.table("new").delete().run(self.conn)


    async def process_one_from_queue(self):
        scrape_req = await self.queue.get()
        await self.http_sem.acquire()
        try:
            await scrape_req.execute()
        finally:
            print("Task done")
            self.queue.task_done()
            self.http_sem.release()

    async def quit_when_done(self):
        await self.queue.join()
        await tornado.gen.sleep(3.0)
        self.loop.stop()


    async def initialize_rethinkdb(self):
        self.r = rethinkdb.RethinkDB()
        self.r.set_loop_type("asyncio")
        self.conn = await self.r.connect()
        print("Init done")

    def get_db(self):
        assert self.r is not None
        assert self.conn is not None

        return self.r, self.conn

    def process_all(self):
        self.loop.spawn_callback(self.quit_when_done)
        #self.loop.start()

class LivescraperDashboardHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("template.html")

class LivescraperWSHandler(tornado.websocket.WebSocketHandler):
    existing_connections = set()
    async def open(self):
        self.existing_connections.add(self)
        print("Now have", len(self.existing_connections), "connections open")

    async def close(self):
        self.existing_connections.remove(self)
        print("Now have", len(self.existing_connections), "connections open")

    async def send_update(self, obj):
        print("Sending an update")
        self.write_message(json.dumps(obj, default=lambda x: x.isoformat()))

    @staticmethod
    async def broadcast_to_all(obj):
        print("BCA called")
        for conn in LivescraperWSHandler.existing_connections:
            tornado.ioloop.IOLoop().current().spawn_callback(conn.send_update, obj)

def make_livescraper_app():
    return tornado.web.Application([
        (r"/websocket", LivescraperWSHandler),
        (r"/(.*)", tornado.web.StaticFileHandler, {"path" : "./static/"}),

    ], compiled_template_cache=False)




async def start_listening(r, conn):


    feed = await r.table("new").changes(squash=True, include_initial=True, include_types=True).run(conn)

    async for change in feed:
        print(change)
        await LivescraperWSHandler.broadcast_to_all(change)



async def parse_reddit_post(response, live_scrape):
    resp_data = json.loads(response.body.decode("utf-8"))
    post, comments = resp_data

    top_level_comments = []
    for top_level_comment in comments["data"]["children"]:
        if top_level_comment["kind"] == "t1":
            comment_body = top_level_comment["data"]["body"]
            top_level_comments.append(comment_body)
            print(len(comment_body))

    return {"comments" : top_level_comments, "title" : post["data"]["children"][0]["data"]["title"]}





async def parse_initial_reddit_page(response, live_scrape):
    resp_data = json.loads(response.body.decode("utf-8"))


    for post in resp_data["data"]["children"]:
        url = post["data"]["url"]
        live_scrape.enqueue_request(url + ".json?", parse_reddit_post)


if __name__ == "__main__":

    livescraper = LiveScrape(project="reddit_test", num_conc_reqs=3, wipe=True)
    r, conn = livescraper.get_db()

    app = make_livescraper_app()
    app.listen(8888)

    livescraper.loop.spawn_callback(start_listening, r, conn)
    livescraper.enqueue_request("https://www.reddit.com/r/askreddit/.json?", parse_initial_reddit_page)
    livescraper.loop.start()

    # app = make_livescraper_app()
    # app.listen(8888)
    # tornado.ioloop.IOLoop().current().start()


    quit()
    #urls = [str(i) for i in range(10)]

    #livescraper = LiveScrape(project="reddit_test", num_conc_reqs=1, wipe=True)


    #livescraper.enqueue_request("https://www.reddit.com/r/askreddit/.json?", parse_initial_reddit_page)

    #livescraper.process_all()

    # loop = tornado.ioloop.IOLoop().current()
    #
    #
    # conc_http_requster = BoundedHTTP(3)
    #
    # for url in urls:
    #     loop.spawn_callback(conc_http_requster.http_request, url)
    #
    # loop.start()

