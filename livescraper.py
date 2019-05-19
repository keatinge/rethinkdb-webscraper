import tornado
import tornado.ioloop
import tornado.web
import tornado.locks
import tornado.queues
import tornado.httpclient
import tornado.websocket
import pytz
import rethinkdb

import json
import datetime

CHROME_UA = "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36"


def now_eastern():
    tz_info = pytz.timezone("US/Eastern")
    return datetime.datetime.now(tz_info)


class LiveScrapeReq:
    def __init__(self, url, after_req_f, live_scrape):
        self.created_at = datetime.datetime.now()
        self.after_req_f = after_req_f
        self.url = url
        self.live_scrape_instance = live_scrape


    async def execute(self):
        # Logging

        self.execute_start = datetime.datetime.now()


        rethink = self.live_scrape_instance.rethink_proxy
        uuid = await rethink.get_uuid_for_url(self.url)
        #response = await tornado.gen.sleep(np.random.normal(loc=1.0, scale=0.2))
        response = await tornado.httpclient.AsyncHTTPClient().fetch(
            self.url, user_agent=CHROME_UA)
        data_to_save = await self.after_req_f(response, self.live_scrape_instance)
        await rethink.save_data_for_uuid(uuid, data_to_save)
        print("Done with", self.url)



class RethinkProxy:
    def __init__(self):
        self.r = rethinkdb.RethinkDB()
        self.r.set_loop_type("asyncio")
        try:
            self.conn = tornado.ioloop.IOLoop().current().run_sync(self.r.connect)
            self.disabled = False
        except rethinkdb.errors.ReqlDriverError as e:
            self.disabled = True
            print("Cannot connect to rethink", e)

        self.table = self.r.table("new")
        print("Init done")
    
    async def get_uuid_for_url(self, url):
        if self.disabled: return None
        
        to_insert_data = {
            "url" : url,
            "created_at" : now_eastern(),
            "completed_at" : None,
            "raw_response" : None,
            "parsed_data" : None
        }

        changes = await self.table.insert(to_insert_data).run(conn)
        uuid = changes["generated_keys"][0]

        return uuid

    async def save_data_for_uuid(self, uuid, data_to_save):
        if self.disabled: return None

        update_data = {
            "completed_at" : now_eastern(),
            #"raw_response" : response.body.decode("utf-8"),
            "parsed_data" : data_to_save
        }
 
        await self.table.get(uuid).update(update_data).run(self.conn)

    async def wipe_table(self):
        if self.disabled: return None
        print("Wiping table")
        await self.table.delete().run(self.conn)

    async def get_live_feed(self):
        if self.disabled:
            return

        feed = await r.table("new") \
            .changes(squash=True, include_initial=True, include_types=True) \
            .run(conn)

        async for el in feed:
            yield el





         

class LiveScrape:
    def __init__(self, project, num_conc_reqs=3, wipe=False):
        self.project = project
        self.http_sem = tornado.locks.Semaphore(num_conc_reqs)
        self.queue = tornado.queues.Queue(maxsize=0)
        self.loop = tornado.ioloop.IOLoop().current()

        self.rethink_proxy = RethinkProxy()

        if wipe:
            self.loop.run_sync(self.rethink_proxy.wipe_table)

    def run_to_completion(self):
        self.loop.spawn_callback(LivescraperWSHandler.start_listening, self.rethink_proxy)

        app = tornado.web.Application([
            (r"/websocket", LivescraperWSHandler),
            (r"/(.*)", tornado.web.StaticFileHandler, {"path" : "./static/"}),
        ], compiled_template_cache=False)

        self.loop.start()
        self.app.run()

    def enqueue_request(self, url, callback):
        self.queue.put(LiveScrapeReq(url, callback, self))
        self.loop.spawn_callback(self.process_one_from_queue)


    
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
        for conn in LivescraperWSHandler.existing_connections:
            tornado.ioloop.IOLoop().current().spawn_callback(conn.send_update, obj)
            
    @staticmethod
    async def start_listening(rethink_proxy):
        async for change in rethink_proxy.get_live_feed():
            await LivescraperWSHandler.broadcast_to_all(change)




