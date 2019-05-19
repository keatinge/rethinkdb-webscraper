import livescraper
import json


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

    live_scrape = livescraper.LiveScrape(project="reddit_test", num_conc_reqs=3, wipe=True)
    live_scrape.enqueue_request("https://www.reddit.com/r/askreddit/.json?",
            parse_initial_reddit_page)

    live_scrape.run_to_completion()


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


