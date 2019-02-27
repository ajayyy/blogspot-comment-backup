import asyncio, aiohttp, json

class MarkExclusion(Exception):
    pass

class NoEntries(Exception):
    pass


async def get_blog_posts(blog, exclusion_limit, session):
    json_loads = json.loads
    # Create a new request session so we can reuse for following requests
    # Results in much faster requests
    session_get = session.get # A minor speed optimization trick
    # Initialize variables for the while loop
    i = 0
    post_urls = []
    post_urls_extend = post_urls.extend
    complete = False
    while not complete:
        index = (i * 150) + 1

        if exclusion_limit and index > exclusion_limit:
            raise MarkExclusion(f"Blog has greater than {exclusion_limit} posts")

        #Can only get 150 blog posts returned even if a higher number is specified
        url = blog + '/feeds/posts/default?max-results=150&alt=json&start-index=' + str(index)
        print("Getting posts from feed: " + url)
        request_info = await session_get(url)
        # Check if the blog exists and is accessible
        if request_info.status == 404: # Blog does not exist
            return "nf" # Blog not found
        elif request_info.status == 401: # Blog is private. Note: Blogs with content warnings do not seem to be blocked by these requests, so this error will *not* appear for those.
            return "pr" # Private blog
        elif request_info.status != 200: # Any other error. Should really retry these requests.
            return "oe" # Other error
        else: # The blog is accessible, proceed in retreiving links
            text = await request_info.text()

            feed_json = None

            for _ in range(5):
                try:
                    feed_json = json_loads(text)
                    break
                except json.decoder.JSONDecodeError:
                    print(f"Unable to load posts as JSON for: {blog}, retrying in 10 seconds")
                    await asyncio.sleep(10)

            if feed_json:
                if "feed" in feed_json and "entry" in feed_json["feed"]:
                    post_urls_extend([feed_json['feed']['entry'][p]['link'][-1]['href'] for p in range(0, len(feed_json['feed']['entry']))])
                    if len(feed_json['feed']['entry']) != 150:
                        complete = True
                    else:
                        i += 1
                elif i == 0:
                    raise NoEntries
                elif i > 0:
                    break
            else:
                raise MarkExclusion("Unable to load response as JSON")

    return post_urls # Return the complete list of articles

async def test():
    async with aiohttp.ClientSession() as session:
        # Sample default blog
        # A trailing slash on the URL seems to work OK, even if it processes with a double slash
        # blog = 'https://blogger.googleblog.com/'#'https://blogger.googleblog.com'#'https://mytriptoamerica.blogspot.com'
        # blog = "https://buzz.blogspot.com/"
        blog = "https://airton-sweetspot.blogspot.com"

        post_urls = await get_blog_posts(blog, 0, session) #Retrieve the sample blog's articles
        print(f"Found {len(post_urls)} post links")
        print(json.dumps(post_urls))

if __name__ == '__main__':
    asyncio.run(test())

# TODO:
# Report any of the above errors
# Check redirects for duplicate domains (if a custom domain is preferred, when searching for a blog by its regular name, it will automatically return the links under the custom domain, possibly redoing/duplicating work.
